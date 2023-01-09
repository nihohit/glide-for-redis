/* tslint:disable */
/* eslint-disable */

/* auto-generated by NAPI-RS */

export const enum RequestType {
  /** Type of a server address request */
  ServerAddress = 1,
  /** Type of a get string request. */
  GetString = 2,
  /** Type of a set string request. */
  SetString = 3
}
export const enum ResponseType {
  /** Type of a response that returns a null. */
  Null = 0,
  /** Type of a response that returns a string. */
  String = 1,
  /** Type of response containing an error that impacts a single request. */
  RequestError = 2,
  /** Type of response containing an error causes the connection to close. */
  ClosingError = 3
}
export const HEADER_LENGTH_IN_BYTES: number
export function StartSocketConnection(): Promise<string>
export function StartSocketLikeConnection(): Promise<SocketLikeClient>
export class AsyncClient {
  static CreateConnection(connectionAddress: string): AsyncClient
  get(key: string): Promise<string | null>
  set(key: string, value: string): Promise<void>
}
export class SocketLikeClient {
  write(buffer: Uint8Array): Promise<number>
  read(buffer: Uint8Array): Promise<[number, number]>
  close(): void
}
