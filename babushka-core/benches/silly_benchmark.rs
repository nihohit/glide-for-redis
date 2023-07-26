// use std::sync::Arc;

// use criterion::{black_box, criterion_group, criterion_main, Criterion};
// use protobuf::Chars;
// use redis::Cmd;

// fn benchmark_internal<R>(c: &mut Criterion, test_group: &str, func: impl Fn(usize) -> R) {
//     let mut group = c.benchmark_group(test_group);
//     group.significance_level(0.01).sample_size(5000);
//     group.bench_function("bench", move |b| b.iter(|| black_box(func(black_box(100)))));
// }

// fn string_of_length(len: usize) -> String {
//     String::from_utf8(vec![48; len]).unwrap()
// }

// fn benchmark_cmd(c: &mut Criterion) {
//     benchmark_internal(c, "cmd", |length| {
//         let mut cmd = Cmd::new();
//         cmd.arg("GET")
//             .arg("SET")
//             .arg("YOU BET")
//             .arg(string_of_length(length));
//         cmd.get_packed_command()
//     });
// }

// fn benchmark_cmd_bytes(c: &mut Criterion) {
//     benchmark_internal(c, "cmd-bytes", |length| {
//         let mut cmd = Cmd::new();
//         cmd.arg("GET")
//             .arg("SET")
//             .arg("YOU BET")
//             .arg(string_of_length(length));
//         cmd.pack_command_to_bytes()
//     });
// }

// fn benchmark_bytes_with_array(c: &mut Criterion) {
//     benchmark_internal(c, "benchmark_bytes_with_array", |length| {
//         let str = string_of_length(length);
//         redis::pack_command_to_bytes(["GET", "SET", "YOU BET", str.as_str()].iter(), None)
//     });
// }

// fn benchmark_bytes_with_command_args_no_name(c: &mut Criterion) {
//     benchmark_internal(c, "benchmark_bytes_with_command_args_no_name", |length| {
//         let str = string_of_length(length);
//         let other_args = ["GET", "SET", "YOU BET", str.as_str()]
//             .into_iter()
//             .map(|str| str.into())
//             .collect();
//         let args = babushka::command_args::CommandArgs {
//             other_args,
//             command_name: None,
//         };
//         redis::pack_command_to_bytes(args.iter(), None)
//     });
// }

// fn benchmark_bytes_with_command_args_with_name(c: &mut Criterion) {
//     benchmark_internal(c, "benchmark_bytes_with_command_args_with_name", |length| {
//         let str = string_of_length(length);
//         let other_args = ["SET", "YOU BET", str.as_str()]
//             .into_iter()
//             .map(|str| str.into())
//             .collect();
//         let args = babushka::command_args::CommandArgs {
//             other_args,
//             command_name: Some("GET"),
//         };
//         redis::pack_command_to_bytes(args.iter(), None)
//     });
// }

// fn benchmark_new_vec_with_array_of_str(c: &mut Criterion) {
//     benchmark_internal(c, "benchmark_new_vec_with_array_of_str", |length| {
//         let str = string_of_length(length);
//         redis::pack_command_to_vec(["GET", "SET", "YOU BET", str.as_str()].iter(), None)
//     });
// }

// fn benchmark_vec_with_array_of_str(c: &mut Criterion) {
//     benchmark_internal(c, "benchmark_vec_with_array_of_str", |length| {
//         let str = string_of_length(length);
//         redis::pack_command(&["GET", "SET", "YOU BET", str.as_str()])
//     });
// }

// fn benchmark_arc_vec_with_array_of_str(c: &mut Criterion) {
//     benchmark_internal(c, "benchmark_vec_with_array_of_str", |length| {
//         let str = string_of_length(length);
//         Arc::new(redis::pack_command(&[
//             "GET",
//             "SET",
//             "YOU BET",
//             str.as_str(),
//         ]))
//     });
// }

// fn benchmark_vec_with_array_of_chars(c: &mut Criterion) {
//     benchmark_internal(c, "benchmark_vec_with_array_of_chars", |length| {
//         let str = string_of_length(length);
//         let other_args: Vec<Chars> = ["GET", "SET", "YOU BET", str.as_str()]
//             .into_iter()
//             .map(|str| str.into())
//             .collect();
//         let other_ref: Vec<&[u8]> = other_args.iter().map(|chars| chars.as_bytes()).collect();

//         redis::pack_command(&other_ref[..])
//     });
// }

// fn benchmark_new_vec_with_command_args_no_name(c: &mut Criterion) {
//     benchmark_internal(c, "benchmark_new_Vec_with_command_args_no_name", |length| {
//         let str = string_of_length(length);
//         let other_args = ["GET", "SET", "YOU BET", str.as_str()]
//             .into_iter()
//             .map(|str| str.into())
//             .collect();
//         let args = babushka::command_args::CommandArgs {
//             other_args,
//             command_name: None,
//         };
//         redis::pack_command_to_vec(args.iter(), None)
//     });
// }

// fn benchmark_arc_vec_with_command_args_no_name(c: &mut Criterion) {
//     benchmark_internal(c, "benchmark_new_Vec_with_command_args_no_name", |length| {
//         let str = string_of_length(length);
//         let other_args = ["GET", "SET", "YOU BET", str.as_str()]
//             .into_iter()
//             .map(|str| str.into())
//             .collect();
//         let args = babushka::command_args::CommandArgs {
//             other_args,
//             command_name: None,
//         };
//         Arc::new(redis::pack_command_to_vec(args.iter(), None))
//     });
// }

// fn benchmark_new_vec_with_command_args_with_name(c: &mut Criterion) {
//     benchmark_internal(
//         c,
//         "benchmark_new_vec_with_command_args_with_name",
//         |length| {
//             let str = string_of_length(length);
//             let other_args = ["SET", "YOU BET", str.as_str()]
//                 .into_iter()
//                 .map(|str| str.into())
//                 .collect();
//             let args = babushka::command_args::CommandArgs {
//                 other_args,
//                 command_name: Some("GET"),
//             };
//             redis::pack_command_to_vec(args.iter(), None)
//         },
//     );
// }

// fn benchmark_arc_vec_with_command_args_with_name(c: &mut Criterion) {
//     benchmark_internal(
//         c,
//         "benchmark_new_vec_with_command_args_with_name",
//         |length| {
//             let str = string_of_length(length);
//             let other_args = ["SET", "YOU BET", str.as_str()]
//                 .into_iter()
//                 .map(|str| str.into())
//                 .collect();
//             let args = babushka::command_args::CommandArgs {
//                 other_args,
//                 command_name: Some("GET"),
//             };
//             Arc::new(redis::pack_command_to_vec(args.iter(), None))
//         },
//     );
// }

// criterion_group!(
//     benches,
//     benchmark_cmd,
//     benchmark_cmd_bytes,
//     benchmark_bytes_with_array,
//     benchmark_bytes_with_command_args_no_name,
//     benchmark_bytes_with_command_args_with_name,
//     benchmark_vec_with_array_of_str,
//     benchmark_vec_with_array_of_chars,
//     benchmark_new_vec_with_array_of_str,
//     benchmark_new_vec_with_command_args_no_name,
//     benchmark_new_vec_with_command_args_with_name
// );

// criterion_main!(benches);

fn main() {}
