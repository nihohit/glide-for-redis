use crate::fake_multiplexer::FakeMultiplexer;

use super::{headers::*, rotating_buffer::RotatingBuffer};
use bytes::BufMut;
use futures::Future;
use num_traits::ToPrimitive;
use redis::RedisResult;
use redis::{Client, RedisError};
use std::cell::RefCell;
use std::cmp::min;
use std::ops::{Deref, DerefMut, Range};
use std::pin::Pin;
use std::rc::Rc;
use std::str;
use std::task::Poll;
use std::{io, thread};
use tokio::runtime::Builder;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::task;
use ClosingReason2::*;
use PipeListeningResult::*;

struct SocketListener {
    write_request_receiver: UnboundedReceiver<SocketWriteRequest>,
    rotating_buffer: RotatingBuffer,
}

enum PipeListeningResult {
    Closed(ClosingReason2),
    ReceivedValues(Vec<WholeRequest>),
}

impl From<ClosingReason2> for PipeListeningResult {
    fn from(result: ClosingReason2) -> Self {
        Closed(result)
    }
}

struct AccumulatedOutputsInternal {
    vec: Vec<u8>,
    waker: Option<futures::task::Waker>,
}
struct AccumulatedOutputs(RefCell<AccumulatedOutputsInternal>);

impl AccumulatedOutputs {
    fn with_vec<ReturnType, Function: FnOnce(&mut Vec<u8>) -> ReturnType>(
        &self,
        action: Function,
    ) -> ReturnType {
        let val = &mut self.0.borrow_mut();
        action(&mut val.vec)
    }

    fn wake(&self) {
        if let Some(waker) = self.0.borrow_mut().waker.take() {
            waker.wake();
        }
    }

    fn set_waker(&self, waker: futures::task::Waker) {
        assert!(self.0.borrow_mut().waker.is_none());
        self.0.borrow_mut().waker = Some(waker);
    }
}

impl SocketListener {
    fn new(read_request_receiver: UnboundedReceiver<SocketWriteRequest>) -> Self {
        let rotating_buffer = RotatingBuffer::new(2, 65_536);
        SocketListener {
            write_request_receiver: read_request_receiver,
            rotating_buffer,
        }
    }

    async fn next_values(&mut self) -> PipeListeningResult {
        loop {
            let Some(read_request) = self.write_request_receiver.recv().await else {
                return ReadSocketClosed.into();
            };
            let reference = read_request.buffer.as_ref().as_ref();
            self.rotating_buffer
                .current_buffer()
                .extend_from_slice(reference);

            let completion = read_request.completion;
            completion(reference.len());

            return match self.rotating_buffer.get_requests() {
                Ok(requests) => ReceivedValues(requests),
                Err(err) => UnhandledError(err.into()).into(),
            };
        }
    }
}

fn write_response_header(
    accumulated_outputs: &Rc<AccumulatedOutputs>,
    callback_index: u32,
    response_type: ResponseType,
    length: usize,
) -> Result<(), io::Error> {
    accumulated_outputs.with_vec(|vec| {
        vec.put_u32_le(length as u32);
        vec.put_u32_le(callback_index);
        vec.put_u32_le(response_type.to_u32().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Response type {:?} wasn't found", response_type),
            )
        })?);

        assert!(!vec.is_empty());
        Ok(())
    })
}

fn write_null_response_header(
    accumulated_outputs: &Rc<AccumulatedOutputs>,
    callback_index: u32,
) -> Result<(), io::Error> {
    write_response_header(
        accumulated_outputs,
        callback_index,
        ResponseType::Null,
        HEADER_END,
    )
}

fn write_slice_to_output(accumulated_outputs: &AccumulatedOutputs, bytes_to_write: &[u8]) {
    accumulated_outputs.with_vec(|vec| vec.extend_from_slice(bytes_to_write));
}

async fn send_set_request(
    buffer: SharedBuffer,
    key_range: Range<usize>,
    value_range: Range<usize>,
    callback_index: u32,
    mut connection: FakeMultiplexer,
    accumulated_outputs: Rc<AccumulatedOutputs>,
) -> RedisResult<()> {
    connection
        .set(&buffer[key_range], &buffer[value_range])
        .await?;
    write_null_response_header(&accumulated_outputs, callback_index)?;
    accumulated_outputs.wake();
    Ok(())
}

async fn send_get_request(
    vec: SharedBuffer,
    key_range: Range<usize>,
    callback_index: u32,
    mut connection: FakeMultiplexer,
    accumulated_outputs: Rc<AccumulatedOutputs>,
) -> RedisResult<()> {
    let result: Option<Vec<u8>> = connection.get(&vec[key_range]).await?;
    match result {
        Some(result_bytes) => {
            let length = HEADER_END + result_bytes.len();
            write_response_header(
                &accumulated_outputs,
                callback_index,
                ResponseType::String,
                length,
            )?;
            write_slice_to_output(&accumulated_outputs, &result_bytes);
        }
        None => {
            write_null_response_header(&accumulated_outputs, callback_index)?;
        }
    };

    accumulated_outputs.wake();
    Ok(())
}

fn handle_request(
    request: WholeRequest,
    connection: FakeMultiplexer,
    accumulated_outputs: Rc<AccumulatedOutputs>,
) {
    task::spawn_local(async move {
        let result = match request.request_type {
            RequestRanges::Get { key: key_range } => {
                send_get_request(
                    request.buffer,
                    key_range,
                    request.callback_index,
                    connection.clone(),
                    accumulated_outputs.clone(),
                )
                .await
            }
            RequestRanges::Set {
                key: key_range,
                value: value_range,
            } => {
                send_set_request(
                    request.buffer,
                    key_range,
                    value_range,
                    request.callback_index,
                    connection.clone(),
                    accumulated_outputs.clone(),
                )
                .await
            }
            RequestRanges::ServerAddress { address: _ } => {
                unreachable!("Server address can only be sent once")
            }
        };
        if let Err(err) = result {
            write_error(
                err,
                request.callback_index,
                ResponseType::RequestError,
                &accumulated_outputs,
            )
            .await;
        }
    });
}

async fn write_error(
    err: RedisError,
    callback_index: u32,
    response_type: ResponseType,
    accumulated_outputs: &Rc<AccumulatedOutputs>,
) {
    let err_str = err.to_string();
    let error_bytes = err_str.as_bytes();
    let length = HEADER_END + error_bytes.len();
    write_response_header(&accumulated_outputs, callback_index, response_type, length)
        .expect("Failed writing error to vec");
    write_slice_to_output(&accumulated_outputs, err.to_string().as_bytes());

    accumulated_outputs.wake();
}

async fn handle_requests(
    received_requests: Vec<WholeRequest>,
    connection: &FakeMultiplexer,
    accumulated_outputs: &Rc<AccumulatedOutputs>,
) {
    for request in received_requests {
        handle_request(request, connection.clone(), accumulated_outputs.clone());
    }
    // Yield to ensure that the subtasks aren't starved.
    task::yield_now().await;
}

fn to_babushka_result<T, E: std::fmt::Display>(
    result: Result<T, E>,
    err_msg: Option<&str>,
) -> Result<T, BabushkaError> {
    result.map_err(|err: E| {
        BabushkaError::BaseError(match err_msg {
            Some(msg) => format!("{}: {}", msg, err),
            None => format!("{}", err),
        })
    })
}

async fn parse_address_create_conn(
    request: &WholeRequest,
    address_range: Range<usize>,
    read_request_receiver: &mut UnboundedReceiver<SocketReadRequest>,
    accumulated_outputs: &Rc<AccumulatedOutputs>,
) -> Result<FakeMultiplexer, BabushkaError> {
    let address = &request.buffer[address_range];
    let address = to_babushka_result(
        std::str::from_utf8(address),
        Some("Failed to parse address"),
    )?;
    let _client = to_babushka_result(
        Client::open(address),
        Some("Failed to open redis-rs client"),
    )?;
    let connection = FakeMultiplexer {};

    let Some(mut write_request) = read_request_receiver.recv().await else {
        return Err(BabushkaError::CloseError(ClosingReason2::AllConnectionsClosed));
    };

    write_null_response_header(accumulated_outputs, request.callback_index)
        .expect("Failed writing address response.");

    let mut reference = write_request.buffer.as_mut().as_mut();

    accumulated_outputs.with_vec(|vec| {
        assert!(vec.len() <= reference.len()); // otherwise write isn't possible.
        let written_bytes = vec.len();
        reference.put(vec.drain(0..written_bytes).as_slice());
        let completion = write_request.completion;
        completion((written_bytes, 0));
    });

    Ok(connection)
}

async fn wait_for_server_address_create_conn(
    client_listener: &mut SocketListener,
    read_request_receiver: &mut UnboundedReceiver<SocketReadRequest>,
    accumulated_outputs: &Rc<AccumulatedOutputs>,
) -> Result<FakeMultiplexer, BabushkaError> {
    // Wait for the server's address
    let request = client_listener.next_values().await;
    match request {
        Closed(reason) => {
            return Err(BabushkaError::CloseError(reason));
        }
        ReceivedValues(received_requests) => {
            if let Some(index) = (0..received_requests.len()).next() {
                let request = received_requests
                    .get(index)
                    .ok_or_else(|| BabushkaError::BaseError("No received requests".to_string()))?;
                match request.request_type.clone() {
                    RequestRanges::ServerAddress {
                        address: address_range,
                    } => {
                        return parse_address_create_conn(
                            request,
                            address_range,
                            read_request_receiver,
                            accumulated_outputs,
                        )
                        .await
                    }
                    _ => {
                        return Err(BabushkaError::BaseError(
                            "Received another request before receiving server address".to_string(),
                        ))
                    }
                }
            }
        }
    }
    Err(BabushkaError::BaseError(
        "Failed to get the server's address".to_string(),
    ))
}

async fn read_values(
    client_listener: &mut SocketListener,
    accumulated_outputs: &Rc<AccumulatedOutputs>,
    connection: FakeMultiplexer,
) -> Result<(), BabushkaError> {
    loop {
        match client_listener.next_values().await {
            Closed(reason) => {
                return Err(BabushkaError::CloseError(reason)); // TODO: implement error protocol, handle error closing reasons
            }
            ReceivedValues(received_requests) => {
                handle_requests(received_requests, &connection, accumulated_outputs).await;
            }
        }
    }
}

struct AccumulatedOutputsWriter<'a> {
    write_request_receiver: &'a mut UnboundedReceiver<SocketReadRequest>,
    write_request: Option<SocketReadRequest>,
    accumulated_outputs: &'a AccumulatedOutputs,
}

impl Future for AccumulatedOutputsWriter<'_> {
    type Output = Result<(), BabushkaError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        if self.write_request.is_none() {
            let Poll::Ready(result) =
            unsafe { Pin::new_unchecked(&mut self.write_request_receiver.recv()) }.poll(cx) else {
                return Poll::Pending;
            };
            let Some(write_request) = result else {
                return Poll::Ready(Err(BabushkaError::CloseError(ClosingReason2::AllConnectionsClosed)));
            };
            self.write_request = Some(write_request);
        }
        let mut write_request = self.write_request.take().unwrap();

        let poll = self.accumulated_outputs.with_vec(|vec| {
            if vec.is_empty() {
                self.write_request = Some(write_request);
                return Poll::Pending;
            }

            let mut reference = write_request.buffer.as_mut().as_mut();

            let bytes_to_write = min(reference.len(), vec.len());
            reference.put(vec.drain(0..bytes_to_write).as_slice());
            let remaining_vec_len = vec.len();

            let completion = write_request.completion;
            completion((bytes_to_write, remaining_vec_len));

            Poll::Ready(Ok(()))
        });
        if poll.is_pending() {
            self.accumulated_outputs.set_waker(cx.waker().clone());
        }
        poll

        // loop {
        //     // looping is required since notified() might have 2 permits - https://github.com/tokio-rs/tokio/pull/5305
        //     loop {
        //         read_possible.notified().await;
        //         let mut vec = accumulated_outputs.get_vec();
        //         // possible in case of 2 permits
        //         if vec.is_empty() {
        //             continue;
        //         }

        //         assert!(!vec.is_empty());
        //         let mut reference = write_request.buffer.as_mut().as_mut();

        //         let bytes_to_write = min(reference.len(), vec.len());
        //         reference.put(vec.drain(0..bytes_to_write).as_slice());
        //         if !vec.is_empty() {
        //             read_possible.notify_one();
        //         }
        //         let remaining_vec_len = vec.len();

        //         let completion = write_request.completion;
        //         completion((bytes_to_write, remaining_vec_len));
        //         break;
        //     }
        // }
    }
}

fn write_accumulated_outputs<'a>(
    write_request_receiver: &'a mut UnboundedReceiver<SocketReadRequest>,
    accumulated_outputs: &'a AccumulatedOutputs,
) -> AccumulatedOutputsWriter<'a> {
    AccumulatedOutputsWriter {
        write_request_receiver,
        write_request: None,
        accumulated_outputs,
    }
}

///
pub type ReadSender = UnboundedSender<SocketReadRequest>;

///
pub type WriteSender = UnboundedSender<SocketWriteRequest>;

async fn listen_on_client_stream(
    mut read_request_receiver: UnboundedReceiver<SocketReadRequest>,
    write_request_receiver: UnboundedReceiver<SocketWriteRequest>,
) -> Result<(), BabushkaError> {
    let mut client_listener = SocketListener::new(write_request_receiver);
    let accumulated_outputs = Rc::new(AccumulatedOutputs {
        0: RefCell::new(AccumulatedOutputsInternal {
            vec: Vec::new(),
            waker: None,
        }),
    });
    let connection = wait_for_server_address_create_conn(
        &mut client_listener,
        &mut read_request_receiver,
        &accumulated_outputs,
    )
    .await
    .unwrap();
    let result = tokio::try_join!(
        read_values(&mut client_listener, &accumulated_outputs, connection),
        write_accumulated_outputs(&mut read_request_receiver, &accumulated_outputs)
    )
    .map(|_| ());
    return result;
}

async fn listen_on_socket<InitCallback>(init_callback: InitCallback)
where
    InitCallback: FnOnce(Result<(WriteSender, ReadSender), RedisError>) + Send + 'static,
{
    let local = task::LocalSet::new();
    let (read_request_sender, read_request_receiver) = unbounded_channel();
    let (write_request_sender, write_request_receiver) = unbounded_channel();
    init_callback(Ok((read_request_sender, write_request_sender)));
    let _ = local
        .run_until(listen_on_client_stream(
            write_request_receiver,
            read_request_receiver,
        ))
        .await;
    println!("RS done listen_on_socket");
}

#[derive(Debug)]
/// Enum describing the reason that a socket listener stopped listening on a socket.
pub enum ClosingReason2 {
    /// The socket was closed. This is usually the required way to close the listener.
    ReadSocketClosed,
    /// The listener encounter an error it couldn't handle.
    UnhandledError(RedisError),
    /// No clients left to handle, close the connection
    AllConnectionsClosed,
}

/// Enum describing babushka errors
#[derive(Debug)]
pub enum BabushkaError {
    /// Base error
    BaseError(String),
    /// Close error
    CloseError(ClosingReason2),
}

///
pub enum SocketRequestType {
    ///
    Read,
    ///
    Write,
}

///
pub struct SocketWriteRequest {
    buffer: Box<dyn Deref<Target = [u8]>>,
    completion: Box<dyn FnOnce(usize)>,
}

///
pub struct SocketReadRequest {
    buffer: Box<dyn DerefMut<Target = [u8]>>,
    completion: Box<dyn FnOnce((usize, usize))>,
}

impl SocketWriteRequest {
    ///
    pub fn new(buffer: Box<dyn Deref<Target = [u8]>>, completion: Box<dyn FnOnce(usize)>) -> Self {
        SocketWriteRequest { buffer, completion }
    }
}

impl SocketReadRequest {
    ///
    pub fn new(
        buffer: Box<dyn DerefMut<Target = [u8]>>,
        completion: Box<dyn FnOnce((usize, usize))>,
    ) -> Self {
        SocketReadRequest { buffer, completion }
    }
}

impl std::fmt::Debug for SocketWriteRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SocketWriteRequest")
            .field("buffer", &self.buffer.as_ptr_range())
            .finish()
    }
}

impl std::fmt::Debug for SocketReadRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SocketReadRequest")
            .field("buffer", &self.buffer.as_ptr_range())
            .finish()
    }
}

/// Creates a new thread with a main loop task listening on the socket for new connections.
/// Every new connection will be assigned with a client-listener task to handle their requests.
///
/// # Arguments
/// * `init_callback` - called when the socket listener fails to initialize, with the reason for the failure.
pub fn start_listener<InitCallback>(init_callback: InitCallback)
where
    InitCallback: FnOnce(Result<(WriteSender, ReadSender), RedisError>) + Send + 'static,
{
    println!("RS start start_listener");
    thread::Builder::new()
        .name("socket_like_listener_thread".to_string())
        .spawn(move || {
            let runtime = Builder::new_current_thread()
                .enable_all()
                .thread_name("socket_like_listener_thread")
                .build();
            match runtime {
                Ok(runtime) => {
                    runtime.block_on(listen_on_socket(init_callback));
                }
                Err(err) => init_callback(Err(err.into())),
            };
            println!("RS done thread");
        })
        .expect("Thread spawn failed. Cannot report error because callback was moved.");
}
