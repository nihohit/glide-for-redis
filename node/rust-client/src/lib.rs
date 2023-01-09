use babushka::headers::HEADER_END;
use babushka::{
    start_listener, start_socket_listener, ReadSender, SocketReadRequest, SocketWriteRequest,
    WriteSender,
};
use napi::bindgen_prelude::ToNapiValue;
use napi::bindgen_prelude::Uint8Array;
use napi::{Env, Error, JsObject, Result, Status};
use napi_derive::napi;
use redis::aio::MultiplexedConnection;
use redis::AsyncCommands;
use std::str;
use tokio::runtime::{Builder, Runtime};

// TODO - this repetition will become unmaintainable. We need to do this in macros.
#[napi]
pub enum RequestType {
    /// Type of a server address request
    ServerAddress = 1,
    /// Type of a get string request.
    GetString = 2,
    /// Type of a set string request.
    SetString = 3,
}

// TODO - this repetition will become unmaintainable. We need to do this in macros.
#[napi]
pub enum ResponseType {
    /// Type of a response that returns a null.
    Null = 0,
    /// Type of a response that returns a string.
    String = 1,
    /// Type of response containing an error that impacts a single request.
    RequestError = 2,
    /// Type of response containing an error causes the connection to close.
    ClosingError = 3,
}

// TODO - this repetition will become unmaintainable. We need to do this in macros.
#[napi]
pub const HEADER_LENGTH_IN_BYTES: u32 = HEADER_END as u32;

#[napi]
struct AsyncClient {
    #[allow(dead_code)]
    connection: MultiplexedConnection,
    runtime: Runtime,
}

fn to_js_error<E: std::error::Error>(err: E) -> Error {
    Error::new(Status::Unknown, err.to_string())
}

fn to_js_result<T, E: std::error::Error>(result: std::result::Result<T, E>) -> Result<T> {
    result.map_err(to_js_error)
}

#[napi]
impl AsyncClient {
    #[napi(js_name = "CreateConnection")]
    #[allow(dead_code)]
    pub fn create_connection(connection_address: String) -> Result<AsyncClient> {
        let runtime = Builder::new_multi_thread()
            .enable_all()
            .worker_threads(1)
            .thread_name("Babushka node thread")
            .build()?;
        let _runtime_handle = runtime.enter();
        let client = to_js_result(redis::Client::open(connection_address))?;
        let connection = to_js_result(runtime.block_on(client.get_multiplexed_async_connection()))?;
        Ok(AsyncClient {
            connection,
            runtime,
        })
    }

    #[napi(ts_return_type = "Promise<string | null>")]
    #[allow(dead_code)]
    pub fn get(&self, env: Env, key: String) -> Result<JsObject> {
        let (deferred, promise) = env.create_deferred()?;

        let mut connection = self.connection.clone();
        self.runtime.spawn(async move {
            let result: Result<Option<String>> = to_js_result(connection.get(key).await);
            match result {
                Ok(value) => deferred.resolve(|_| Ok(value)),
                Err(e) => deferred.reject(e),
            }
        });

        Ok(promise)
    }

    #[napi(ts_return_type = "Promise<void>")]
    #[allow(dead_code)]
    pub fn set(&self, env: Env, key: String, value: String) -> Result<JsObject> {
        let (deferred, promise) = env.create_deferred()?;

        let mut connection = self.connection.clone();
        self.runtime.spawn(async move {
            let result: Result<()> = to_js_result(connection.set(key, value).await);
            match result {
                Ok(_) => deferred.resolve(|_| Ok(())),
                Err(e) => deferred.reject(e),
            }
        });

        Ok(promise)
    }
}

#[napi(js_name = "StartSocketConnection", ts_return_type = "Promise<string>")]
pub fn start_socket_listener_external(env: Env) -> Result<JsObject> {
    let (deferred, promise) = env.create_deferred()?;

    start_socket_listener(move |result| {
        match result {
            Ok(path) => deferred.resolve(|_| Ok(path)),
            Err(e) => deferred.reject(to_js_error(e)),
        };
    });

    Ok(promise)
}

#[napi]
struct SocketLikeClient {
    #[allow(dead_code)]
    write_sender: Option<WriteSender>,
    read_sender: Option<ReadSender>,
}

#[napi]
impl SocketLikeClient {
    fn new(write_sender: WriteSender, read_sender: ReadSender) -> SocketLikeClient {
        SocketLikeClient {
            write_sender: Some(write_sender),
            read_sender: Some(read_sender),
        }
    }

    #[napi(ts_return_type = "Promise<number>")]
    #[allow(dead_code)]
    pub fn write(&self, env: Env, buffer: Uint8Array) -> Result<JsObject> {
        let (deferred, promise) = env.create_deferred()?;
        let request = SocketWriteRequest::new(
            Box::new(buffer),
            Box::new(move |size| {
                deferred.resolve(move |_| Ok(size as u32));
            }),
        );
        let _ = self.write_sender.as_ref().unwrap().send(request);

        Ok(promise)
    }

    #[napi(ts_return_type = "Promise<[number, number]>")]
    #[allow(dead_code)]
    pub fn read(&self, env: Env, buffer: Uint8Array) -> Result<JsObject> {
        let (deferred, promise) = env.create_deferred()?;
        let request = SocketReadRequest::new(
            Box::new(buffer),
            Box::new(move |(size, remaining)| {
                let arr = [size as u32, remaining as u32];
                deferred.resolve(move |_| Ok(arr));
            }),
        );
        let _ = self.read_sender.as_ref().unwrap().send(request);

        Ok(promise)
    }

    #[napi]
    #[allow(dead_code)]
    pub fn close(&mut self) {
        println!("closing");
        self.read_sender = None;
        self.write_sender = None;
    }
}

#[napi(
    js_name = "StartSocketLikeConnection",
    ts_return_type = "Promise<SocketLikeClient>"
)]
pub fn start_socket_like_listener_external(env: Env) -> Result<JsObject> {
    let (deferred, promise) = env.create_deferred()?;

    start_listener(move |result| {
        match result {
            Ok((read_sender, write_sender)) => {
                deferred.resolve(|_| Ok(SocketLikeClient::new(read_sender, write_sender)))
            }
            Err(err) => deferred.reject(to_js_error(err)),
        };
    });

    Ok(promise)
}
