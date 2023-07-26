use std::{iter::FusedIterator, ops::Deref};

use protobuf::Chars;

use crate::{
    redis_request::{command, Command, RequestType},
    ClienUsageError, ClientUsageResult,
};

fn command_name(command: &Command) -> Result<Option<&'static str>, ClienUsageError> {
    let request_enum = command
        .request_type
        .enum_value_or(RequestType::InvalidRequest);
    match request_enum {
        RequestType::CustomCommand => Ok(None),
        RequestType::GetString => Ok(Some("GET")),
        RequestType::SetString => Ok(Some("SET")),
        _ => Err(ClienUsageError::InternalError(format!(
            "Received invalid request type: {:?}",
            command.request_type
        ))),
    }
}

pub struct CommandArgs<T: Deref<Target = str> + Clone> {
    pub command_name: Option<&'static str>,
    pub other_args: Vec<T>,
}

#[derive(Clone)]
pub struct ArgsIter<'a, T: Deref<Target = str> + Clone> {
    command_args: &'a CommandArgs<T>,
    index: usize,
}

impl<'a, T: Deref<Target = str> + Clone> ExactSizeIterator for ArgsIter<'a, T> {
    fn len(&self) -> usize {
        self.command_args.other_args.len()
            + if self.command_args.command_name.is_some() {
                1
            } else {
                0
            }
            - self.index
    }
}

impl<'a, T: Deref<Target = str> + Clone> Iterator for ArgsIter<'a, T> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.len() == 0 {
            return None;
        }
        self.index += 1;
        let index = self.index;
        if let Some(cmd_name) = self.command_args.command_name {
            if index == 1 {
                return Some(cmd_name.as_bytes());
            }
            return Some(self.command_args.other_args[index - 2].as_bytes());
        }

        return Some(self.command_args.other_args[index - 1].as_bytes());
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
            command_name: None,
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
    fn arg_idx(&self, idx: usize) -> Option<&[u8]> {
        if idx == 0 {
            if let Some(command_name) = self.command_name {
                return Some(command_name.as_bytes());
            }
        }
        let index = if self.command_name.is_some() {
            idx - 1
        } else {
            idx
        };
        self.other_args.get(index).map(|char| char.as_bytes())
    }

    fn position(&self, candidate: &[u8]) -> Option<usize> {
        if let Some(command_name) = self.command_name {
            if command_name.as_bytes() == candidate {
                return Some(0);
            }
        }
        self.other_args
            .iter()
            .position(|arg| arg.as_bytes() == candidate)
            .map(|position| {
                if self.command_name.is_some() {
                    position + 1
                } else {
                    position
                }
            })
    }
}
