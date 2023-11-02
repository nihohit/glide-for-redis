use std::{iter::FusedIterator, ops::Deref};

use protobuf::Chars;
use redis::cluster_routing::Routable;

use crate::{
    redis_request::{command, Command, RequestType},
    ClienUsageError, ClientUsageResult,
};

#[derive(Clone)]
enum CommandName {
    None,
    Single(&'static str),
    Double((&'static str, &'static str)),
}

impl CommandName {
    fn num_of_args(&self) -> usize {
        match self {
            CommandName::None => 0,
            CommandName::Single(_) => 1,
            CommandName::Double(_) => 2,
        }
    }
}

fn command_name(command: &Command) -> Result<CommandName, ClienUsageError> {
    let request_enum = command
        .request_type
        .enum_value_or(RequestType::InvalidRequest);
    match request_enum {
        RequestType::CustomCommand => Ok(CommandName::None),
        RequestType::GetString => Ok(CommandName::Single("GET")),
        RequestType::SetString => Ok(CommandName::Single("SET")),
        RequestType::Ping => Ok(CommandName::Single("PING")),
        RequestType::Info => Ok(CommandName::Single("INFO")),
        RequestType::Del => Ok(CommandName::Single("DEL")),
        RequestType::Select => Ok(CommandName::Single("SELECT")),
        RequestType::ConfigGet => Ok(CommandName::Double(("CONFIG", "GET"))),
        RequestType::ConfigSet => Ok(CommandName::Double(("CONFIG", "SET"))),
        RequestType::ConfigResetStat => Ok(CommandName::Double(("CONFIG", "RESETSTAT"))),
        RequestType::ConfigRewrite => Ok(CommandName::Double(("CONFIG", "REWRITE"))),
        RequestType::ClientGetName => Ok(CommandName::Double(("CLIENT", "GETNAME"))),
        RequestType::ClientGetRedir => Ok(CommandName::Double(("CLIENT", "GETREDIR"))),
        RequestType::ClientId => Ok(CommandName::Double(("CLIENT", "ID"))),
        RequestType::ClientInfo => Ok(CommandName::Double(("CLIENT", "INFO"))),
        RequestType::ClientKill => Ok(CommandName::Double(("CLIENT", "KILL"))),
        RequestType::ClientList => Ok(CommandName::Double(("CLIENT", "LIST"))),
        RequestType::ClientNoEvict => Ok(CommandName::Double(("CLIENT", "NO-EVICT"))),
        RequestType::ClientNoTouch => Ok(CommandName::Double(("CLIENT", "NO-TOUCH"))),
        RequestType::ClientPause => Ok(CommandName::Double(("CLIENT", "PAUSE"))),
        RequestType::ClientReply => Ok(CommandName::Double(("CLIENT", "REPLY"))),
        RequestType::ClientSetInfo => Ok(CommandName::Double(("CLIENT", "SETINFO"))),
        RequestType::ClientSetName => Ok(CommandName::Double(("CLIENT", "SETNAME"))),
        RequestType::ClientUnblock => Ok(CommandName::Double(("CLIENT", "UNBLOCK"))),
        RequestType::ClientUnpause => Ok(CommandName::Double(("CLIENT", "UNPAUSE"))),
        RequestType::Expire => Ok(CommandName::Single("EXPIRE")),
        RequestType::HashSet => Ok(CommandName::Single("HSET")),
        RequestType::HashGet => Ok(CommandName::Single("HGET")),
        RequestType::HashDel => Ok(CommandName::Single("HDEL")),
        RequestType::HashExists => Ok(CommandName::Single("HEXISTS")),
        RequestType::MSet => Ok(CommandName::Single("MSET")),
        RequestType::MGet => Ok(CommandName::Single("MGET")),
        RequestType::Incr => Ok(CommandName::Single("INCR")),
        RequestType::IncrBy => Ok(CommandName::Single("INCRBY")),
        RequestType::IncrByFloat => Ok(CommandName::Single("INCRBYFLOAT")),
        RequestType::Decr => Ok(CommandName::Single("DECR")),
        RequestType::DecrBy => Ok(CommandName::Single("DECRBY")),
        RequestType::HashGetAll => Ok(CommandName::Single("HGETALL")),
        RequestType::HashMSet => Ok(CommandName::Single("HMSET")),
        RequestType::HashMGet => Ok(CommandName::Single("HMGET")),
        RequestType::HashIncrBy => Ok(CommandName::Single("HINCRBY")),
        RequestType::HashIncrByFloat => Ok(CommandName::Single("HINCRBYFLOAT")),
        RequestType::LPush => Ok(CommandName::Single("LPUSH")),
        RequestType::LPop => Ok(CommandName::Single("LPOP")),
        RequestType::RPush => Ok(CommandName::Single("RPUSH")),
        RequestType::RPop => Ok(CommandName::Single("RPOP")),
        RequestType::LLen => Ok(CommandName::Single("LLEN")),
        RequestType::LRem => Ok(CommandName::Single("LREM")),
        RequestType::LRange => Ok(CommandName::Single("LRANGE")),
        RequestType::LTrim => Ok(CommandName::Single("LTRIM")),
        RequestType::SAdd => Ok(CommandName::Single("SADD")),
        RequestType::SRem => Ok(CommandName::Single("SREM")),
        RequestType::SMembers => Ok(CommandName::Single("SMEMBERS")),
        RequestType::SCard => Ok(CommandName::Single("SCARD")),
        RequestType::PExpireAt => Ok(CommandName::Single("PEXPIREAT")),
        RequestType::PExpire => Ok(CommandName::Single("PEXPIRE")),
        RequestType::ExpireAt => Ok(CommandName::Single("EXPIREAT")),
        RequestType::Exists => Ok(CommandName::Single("EXISTS")),
        RequestType::Unlink => Ok(CommandName::Single("UNLINK")),
        RequestType::TTL => Ok(CommandName::Single("TTL")),
        _ => Err(ClienUsageError::InternalError(format!(
            "Received invalid request type: {:?}",
            command.request_type
        ))),
    }
}

#[derive(Clone)]
pub struct CommandArgs<T: Deref<Target = str> + Clone> {
    command_name: CommandName,
    other_args: Vec<T>,
}

#[derive(Clone)]
pub struct ArgsIter<'a, T: Deref<Target = str> + Clone> {
    command_args: &'a CommandArgs<T>,
    index: usize,
}

impl<T: Deref<Target = str> + Clone> CommandArgs<T> {
    fn len(&self) -> usize {
        self.other_args.len() + self.command_name.num_of_args()
    }
}

impl<'a, T: Deref<Target = str> + Clone> ExactSizeIterator for ArgsIter<'a, T> {
    fn len(&self) -> usize {
        self.command_args.len() - self.index
    }
}

impl<'a, T: Deref<Target = str> + Clone> Iterator for ArgsIter<'a, T> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        let value = self.command_args.arg_idx(self.index);
        if value.is_some() {
            self.index += 1;
        }
        value
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let length = self.len();
        (length, Some(length))
    }
}

fn args_vec(command: Command) -> ClientUsageResult<Vec<Chars>> {
    match command.args {
        Some(command::Args::ArgsArray(args_vec)) => Ok(args_vec.args),
        Some(command::Args::ArgsVecPointer(pointer)) => {
            let string_vec = *unsafe { Box::from_raw(pointer as *mut Vec<String>) };
            let char_vec = string_vec.into_iter().map(|str| str.into()).collect();
            Ok(char_vec)
        }
        None => Err(ClienUsageError::InternalError(
            "Failed to get request arguemnts, no arguments are set".to_string(),
        )),
    }
}

impl CommandArgs<String> {
    pub fn new(other_args: Vec<String>) -> Self {
        Self {
            command_name: CommandName::None,
            other_args,
        }
    }
}

impl<T: Deref<Target = str> + Clone> CommandArgs<T> {
    pub fn iter(&self) -> ArgsIter<T> {
        ArgsIter {
            command_args: self,
            index: 0,
        }
    }
}

pub fn command_args(command: Command) -> ClientUsageResult<CommandArgs<Chars>> {
    let command_name = command_name(&command)?;
    let other_args = args_vec(command)?;

    Ok(CommandArgs {
        command_name,
        other_args,
    })
}

impl<'a, T: Deref<Target = str> + Clone> FusedIterator for ArgsIter<'a, T> {}

impl<T: Deref<Target = str> + Clone> redis::cluster_routing::Routable for CommandArgs<T> {
    fn arg_idx(&self, index: usize) -> Option<&[u8]> {
        if self.len() <= index {
            return None;
        }
        match (&self.command_name, index) {
            (CommandName::Single(name), 0) => Some(name.as_bytes()),
            (CommandName::Double((first, _)), 0) => Some(first.as_bytes()),
            (CommandName::Double((_, second)), 1) => Some(second.as_bytes()),
            _ => {
                let index = index - self.command_name.num_of_args();
                Some(self.other_args[index].as_bytes())
            }
        }
    }

    fn position(&self, candidate: &[u8]) -> Option<usize> {
        match self.command_name {
            CommandName::None => {}
            CommandName::Single(name) => {
                if name.as_bytes() == candidate {
                    return Some(0);
                }
            }
            CommandName::Double((first, second)) => {
                if first.as_bytes() == candidate {
                    return Some(0);
                }
                if second.as_bytes() == candidate {
                    return Some(1);
                }
            }
        }

        self.other_args
            .iter()
            .position(|arg| arg.as_bytes() == candidate)
            .map(|position| position + self.command_name.num_of_args())
    }
}
