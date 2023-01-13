import { BabushkaInternal, SocketLikeClient } from "../";
const {
    StartSocketLikeConnection,
    HEADER_LENGTH_IN_BYTES,
    ResponseType,
    RequestType,
} = BabushkaInternal;

type RequestType = BabushkaInternal.RequestType;
type ResponseType = BabushkaInternal.ResponseType;
type PromiseFunction = (value?: any) => void;

type WriteRequest = {
    callbackIndex: number;
    args: string[];
    type: RequestType;
};

export class SocketLikeConnection {
    private socket: SocketLikeClient | undefined;
    private readonly promiseCallbackFunctions: [
        PromiseFunction,
        PromiseFunction
    ][] = [];
    private readonly availableCallbackSlots: number[] = [];
    private readonly encoder = new TextEncoder();
    private backingReadBuffer = new Uint8Array(4048);
    private backingWriteBuffer = new ArrayBuffer(1024);
    private bufferedWriteRequests: WriteRequest[] = [];
    private writeInProgress = false;

    private increaseReadBufferSize(requiredLength: number) {
        this.backingReadBuffer = new Uint8Array(requiredLength);
    }

    private async read() {
        if (!this.socket) {
            return;
        }
        const [bytesRead, bytesRemaining] = await this.socket.read(
            this.backingReadBuffer
        );
        // console.log(`${bytesRead} ${bytesRemaining}`);

        let counter = 0;
        while (counter <= bytesRead - HEADER_LENGTH_IN_BYTES) {
            const header = new DataView(
                this.backingReadBuffer.buffer,
                counter,
                12
            );
            const length = header.getUint32(0, true);
            if (length === 0) {
                throw new Error("length 0");
            }
            if (counter + length > bytesRead) {
                throw new Error(
                    `received partial message of size ${length}, at ${counter}, read only ${bytesRead}`
                );
            }
            const callbackIndex = header.getUint32(4, true);
            const responseType = header.getUint32(8, true) as ResponseType;
            if (callbackIndex === undefined) {
                throw new Error("Callback is undefined");
            }
            if (this.promiseCallbackFunctions === undefined) {
                throw new Error("promiseCallbackFunctions is undefined");
            }
            if (this.promiseCallbackFunctions[callbackIndex] === undefined) {
                throw new Error(
                    `JS no results for ${callbackIndex} when parsing from ${counter}`
                );
            }
            const [resolve, reject] =
                this.promiseCallbackFunctions[callbackIndex];
            this.availableCallbackSlots.push(callbackIndex);
            if (responseType === ResponseType.Null) {
                resolve(null);
            } else {
                const valueLength = length - HEADER_LENGTH_IN_BYTES;
                const keyBytes = Buffer.from(
                    this.backingReadBuffer.buffer,
                    counter + HEADER_LENGTH_IN_BYTES,
                    valueLength
                );
                const message = keyBytes.toString("utf8");
                if (responseType === ResponseType.String) {
                    resolve(message);
                } else if (responseType === ResponseType.RequestError) {
                    reject(message);
                } else if (responseType === ResponseType.ClosingError) {
                    this.dispose(message);
                }
            }
            counter = counter + length;
        }

        if (this.backingReadBuffer.byteLength < bytesRemaining) {
            this.increaseReadBufferSize(bytesRemaining);
        }
    }

    private async loopReading() {
        while (this.socket) {
            await this.read();
        }
    }

    private constructor(socket: SocketLikeClient) {
        this.socket = socket;
        this.loopReading();
    }

    private getCallbackIndex(): number {
        return (
            this.availableCallbackSlots.pop() ??
            this.promiseCallbackFunctions.length
        );
    }

    private writeHeaderToWriteBuffer(
        length: number,
        callbackIndex: number,
        operationType: RequestType,
        headerLength: number,
        argLengths: number[],
        offset: number
    ) {
        const headerView = new DataView(
            this.backingWriteBuffer,
            offset,
            headerLength
        );
        headerView.setUint32(0, length, true);
        headerView.setUint32(4, callbackIndex, true);
        headerView.setUint32(8, operationType, true);

        for (let i = 0; i < argLengths.length - 1; i++) {
            const argLength = argLengths[i];
            headerView.setUint32(
                HEADER_LENGTH_IN_BYTES + 4 * i,
                argLength,
                true
            );
        }
    }

    private getHeaderLength(writeRequest: WriteRequest) {
        return HEADER_LENGTH_IN_BYTES + 4 * (writeRequest.args.length - 1);
    }

    private lengthOfStrings(request: WriteRequest) {
        return request.args.reduce((sum, arg) => sum + arg.length, 0);
    }

    private encodeStringToWriteBuffer(str: string, byteOffset: number): number {
        const encodeResult = this.encoder.encodeInto(
            str,
            new Uint8Array(this.backingWriteBuffer, byteOffset)
        );
        return encodeResult.written ?? 0;
    }

    private getRequiredBufferLength(writeRequests: WriteRequest[]): number {
        return writeRequests.reduce((sum, request) => {
            return (
                sum +
                this.getHeaderLength(request) +
                // length * 3 is the maximum ratio between UTF16 byte count to UTF8 byte count.
                // TODO - in practice we used a small part of our arrays, and this will be very expensive on
                // large inputs. We can use the slightly slower Buffer.byteLength on longer strings.
                this.lengthOfStrings(request) * 3
            );
        }, 0);
    }

    private async writeBufferedRequestsToSocket() {
        if (!this.socket) {
            return;
        }
        this.writeInProgress = true;
        const writeRequests = this.bufferedWriteRequests.splice(
            0,
            this.bufferedWriteRequests.length
        );
        const requiredBufferLength =
            this.getRequiredBufferLength(writeRequests);

        if (
            !this.backingWriteBuffer ||
            this.backingWriteBuffer.byteLength < requiredBufferLength
        ) {
            this.backingWriteBuffer = new ArrayBuffer(requiredBufferLength);
        }
        let cursor = 0;
        for (const writeRequest of writeRequests) {
            const headerLength = this.getHeaderLength(writeRequest);
            let argOffset = 0;
            const writtenLengths = [];
            for (let arg of writeRequest.args) {
                const argLength = this.encodeStringToWriteBuffer(
                    arg,
                    cursor + headerLength + argOffset
                );
                argOffset += argLength;
                writtenLengths.push(argLength);
            }

            const length = headerLength + argOffset;
            this.writeHeaderToWriteBuffer(
                length,
                writeRequest.callbackIndex,
                writeRequest.type,
                headerLength,
                writtenLengths,
                cursor
            );
            cursor += length;
        }

        const uint8Array = new Uint8Array(this.backingWriteBuffer, 0, cursor);
        await this.socket.write(uint8Array);
        if (this.bufferedWriteRequests.length > 0) {
            await this.writeBufferedRequestsToSocket();
        } else {
            this.writeInProgress = false;
        }
    }

    private writeOrBufferRequest(writeRequest: WriteRequest) {
        this.bufferedWriteRequests.push(writeRequest);
        if (this.writeInProgress) {
            return;
        }
        this.writeBufferedRequestsToSocket();
    }

    public get(key: string): Promise<string> {
        return new Promise(async (resolve, reject) => {
            const callbackIndex = this.getCallbackIndex();
            this.promiseCallbackFunctions[callbackIndex] = [resolve, reject];
            this.writeOrBufferRequest({
                args: [key],
                type: RequestType.GetString,
                callbackIndex,
            });
        });
    }

    public set(key: string, value: string): Promise<void> {
        return new Promise(async (resolve, reject) => {
            const callbackIndex = this.getCallbackIndex();
            this.promiseCallbackFunctions[callbackIndex] = [resolve, reject];
            this.writeOrBufferRequest({
                args: [key, value],
                type: RequestType.SetString,
                callbackIndex,
            });
        });
    }

    private setServerAddress(address: string): Promise<void> {
        return new Promise((resolve, reject) => {
            const callbackIndex = this.getCallbackIndex();
            this.promiseCallbackFunctions[callbackIndex] = [resolve, reject];
            this.writeOrBufferRequest({
                args: [address],
                type: RequestType.ServerAddress,
                callbackIndex,
            });
        });
    }

    public dispose(errorMessage?: string): void {
        this.promiseCallbackFunctions.forEach(([_resolve, reject], _index) => {
            reject(errorMessage);
        });
        if (this.socket) {
            this.socket.close();
            this.socket = undefined;
        }
    }

    public static async CreateConnection(
        address: string
    ): Promise<SocketLikeConnection> {
        const socket = await StartSocketLikeConnection();
        const connection = new SocketLikeConnection(socket);
        await connection.setServerAddress(address);
        return connection;
    }
}
