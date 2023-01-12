use super::{headers::*, rotating_buffer::RotatingBuffer};
use bytes::BufMut;
use num_traits::ToPrimitive;
use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, RedisResult};
use redis::{Client, RedisError};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::ops::{Deref, DerefMut, Range};
use std::rc::Rc;
use std::str;
use std::{io, thread};
use tokio::runtime::Builder;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::Notify;
use tokio::task;
use ClosingReason2::*;
use PipeListeningResult::*;

struct SocketListener {
    write_request_receiver: UnboundedReceiver<SocketWriteRequest>,
    rotating_buffer: RotatingBuffer,
    values_written_notifier: Rc<Notify>,
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

struct OutputAccumulator {
    buffers: VecDeque<Vec<u8>>,
    pool: Vec<Vec<u8>>,
    length: usize,
}

impl OutputAccumulator {
    fn new() -> Self {
        OutputAccumulator {
            buffers: VecDeque::new(),
            pool: Vec::new(),
            length: 0,
        }
    }

    fn add_output(&mut self, output: Vec<u8>) {
        self.length += output.len();
        self.buffers.push_back(output);
    }

    fn get_free_vec(&mut self) -> Vec<u8> {
        self.pool.pop().unwrap_or_default()
    }

    fn borrow_last_output(&mut self) -> &mut Vec<u8> {
        self.buffers.front_mut().unwrap()
    }

    fn is_empty(&self) -> bool {
        self.buffers.is_empty()
    }

    fn fill_buffer(&mut self, buffer: &mut impl BufMut) -> (usize, usize) {
        let mut written_bytes = 0;
        println!("start fill with {} {}", self.buffers.len(), self.length);
        loop {
            let Some(vec_ref) =  self.buffers.front() else {
                break;
            };
            println!("writing {} to {}", vec_ref.len(), buffer.remaining_mut());
            if vec_ref.len() + written_bytes > buffer.remaining_mut() {
                break;
            }
            let mut last = self.buffers.pop_front().unwrap();
            buffer.put(last.as_slice());
            written_bytes += last.len();
            last.clear();
            self.pool.push(last);
        }
        self.length -= written_bytes;

        (written_bytes, self.length)
    }
}

type AccumulatedOutputs = RefCell<OutputAccumulator>;

impl SocketListener {
    fn new(
        read_request_receiver: UnboundedReceiver<SocketWriteRequest>,
        values_written_notifier: Rc<Notify>,
    ) -> Self {
        let rotating_buffer = RotatingBuffer::new(2, 65_536);
        SocketListener {
            write_request_receiver: read_request_receiver,
            rotating_buffer,
            values_written_notifier,
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
    let mut vec = accumulated_outputs.borrow_mut().get_free_vec();
    vec.put_u32_le(length as u32);
    vec.put_u32_le(callback_index);
    vec.put_u32_le(response_type.to_u32().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Response type {:?} wasn't found", response_type),
        )
    })?);

    assert!(!vec.is_empty());
    accumulated_outputs.borrow_mut().add_output(vec);
    Ok(())
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
    let mut accumulated_outputs = accumulated_outputs.borrow_mut();
    let vec = accumulated_outputs.borrow_last_output();
    vec.extend_from_slice(bytes_to_write);
}

async fn send_set_request(
    buffer: SharedBuffer,
    key_range: Range<usize>,
    value_range: Range<usize>,
    callback_index: u32,
    mut connection: MultiplexedConnection,
    accumulated_outputs: Rc<AccumulatedOutputs>,
    values_written_notifier: Rc<Notify>,
) -> RedisResult<()> {
    connection
        .set(&buffer[key_range], &buffer[value_range])
        .await?;
    write_null_response_header(&accumulated_outputs, callback_index)?;
    values_written_notifier.notify_one();
    Ok(())
}

async fn send_get_request(
    vec: SharedBuffer,
    key_range: Range<usize>,
    callback_index: u32,
    mut connection: MultiplexedConnection,
    accumulated_outputs: Rc<AccumulatedOutputs>,
    values_written_notifier: Rc<Notify>,
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

    values_written_notifier.notify_one();
    Ok(())
}

fn handle_request(
    request: WholeRequest,
    connection: MultiplexedConnection,
    accumulated_outputs: Rc<AccumulatedOutputs>,
    values_written_notifier: Rc<Notify>,
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
                    values_written_notifier.clone(),
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
                    values_written_notifier.clone(),
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
                &values_written_notifier,
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
    values_written_notifier: &Rc<Notify>,
) {
    let err_str = err.to_string();
    let error_bytes = err_str.as_bytes();
    let length = HEADER_END + error_bytes.len();
    write_response_header(&accumulated_outputs, callback_index, response_type, length)
        .expect("Failed writing error to vec");
    write_slice_to_output(&accumulated_outputs, err.to_string().as_bytes());

    values_written_notifier.notify_one();
}

async fn handle_requests(
    received_requests: Vec<WholeRequest>,
    connection: &MultiplexedConnection,
    accumulated_outputs: &Rc<AccumulatedOutputs>,
    values_written_notifier: &Rc<Notify>,
) {
    for request in received_requests {
        handle_request(
            request,
            connection.clone(),
            accumulated_outputs.clone(),
            values_written_notifier.clone(),
        );
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
) -> Result<MultiplexedConnection, BabushkaError> {
    let address = &request.buffer[address_range];
    let address = to_babushka_result(
        std::str::from_utf8(address),
        Some("Failed to parse address"),
    )?;
    let client = to_babushka_result(
        Client::open(address),
        Some("Failed to open redis-rs client"),
    )?;
    let connection = to_babushka_result(
        client.get_multiplexed_async_connection().await,
        Some("Failed to create a multiplexed connection"),
    )?;

    let Some(mut write_request) = read_request_receiver.recv().await else {
        return Err(BabushkaError::CloseError(ClosingReason2::AllConnectionsClosed));
    };

    write_null_response_header(accumulated_outputs, request.callback_index)
        .expect("Failed writing address response.");

    let mut reference = write_request.buffer.as_mut().as_mut();

    let mut accumulated_outputs = accumulated_outputs.borrow_mut();
    let result = accumulated_outputs.fill_buffer(&mut reference);

    let completion = write_request.completion;
    completion(result);

    Ok(connection)
}

async fn wait_for_server_address_create_conn(
    client_listener: &mut SocketListener,
    read_request_receiver: &mut UnboundedReceiver<SocketReadRequest>,
    accumulated_outputs: &Rc<AccumulatedOutputs>,
) -> Result<MultiplexedConnection, BabushkaError> {
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
    connection: MultiplexedConnection,
) -> Result<(), BabushkaError> {
    loop {
        match client_listener.next_values().await {
            Closed(reason) => {
                return Err(BabushkaError::CloseError(reason)); // TODO: implement error protocol, handle error closing reasons
            }
            ReceivedValues(received_requests) => {
                handle_requests(
                    received_requests,
                    &connection,
                    accumulated_outputs,
                    &client_listener.values_written_notifier,
                )
                .await;
            }
        }
    }
}

async fn write_accumulated_outputs(
    write_request_receiver: &mut UnboundedReceiver<SocketReadRequest>,
    accumulated_outputs: &AccumulatedOutputs,
    read_possible: &Rc<Notify>,
) -> Result<(), BabushkaError> {
    loop {
        let Some(mut write_request) = write_request_receiver.recv().await else {
            return Err(BabushkaError::CloseError(ClosingReason2::AllConnectionsClosed));
        };
        // looping is required since notified() might have 2 permits - https://github.com/tokio-rs/tokio/pull/5305
        loop {
            read_possible.notified().await;
            let mut accumulated_outputs = accumulated_outputs.borrow_mut();
            // possible in case of 2 permits
            if accumulated_outputs.is_empty() {
                continue;
            }

            assert!(!accumulated_outputs.is_empty());
            let mut reference = write_request.buffer.as_mut().as_mut();

            let result = accumulated_outputs.fill_buffer(&mut reference);
            if !accumulated_outputs.is_empty() {
                read_possible.notify_one();
            }

            let completion = write_request.completion;
            completion(result);
            break;
        }
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
    let notifier = Rc::new(Notify::new());
    let mut client_listener = SocketListener::new(write_request_receiver, notifier.clone());
    let accumulated_outputs = Rc::new(RefCell::new(OutputAccumulator::new()));
    let connection = wait_for_server_address_create_conn(
        &mut client_listener,
        &mut read_request_receiver,
        &accumulated_outputs,
    )
    .await
    .unwrap();
    let result = tokio::try_join!(
        read_values(&mut client_listener, &accumulated_outputs, connection),
        write_accumulated_outputs(&mut read_request_receiver, &accumulated_outputs, &notifier)
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
