import { BabushkaInternal, SocketLikeClient, OpsLogger } from "../";
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
    private backingReadBuffer = new Uint8Array(1024);
    private backingWriteBuffer = new Uint8Array(1024);
    private bufferedWriteRequests: WriteRequest[] = [];
    private writeInProgress = false;
    private remainingReadData: Uint8Array | undefined;
    private logger = new OpsLogger();

    private moveRemainingReadData(buffer: ArrayBuffer) {
        if (!this.remainingReadData) {
            return;
        }
        const array = new Uint8Array(buffer, 0, buffer.byteLength);
        array.set(this.remainingReadData);
        this.remainingReadData = new Uint8Array(
            buffer,
            0,
            this.remainingReadData.length
        );
    }

    private increaseReadBufferSize(requiredAdditionalLength: number) {
        const newBuffer = new Uint8Array(
            this.backingReadBuffer.byteLength + requiredAdditionalLength
        );
        this.moveRemainingReadData(newBuffer);

        this.backingReadBuffer = newBuffer;
        this.socket?.setReadBuffer(this.backingReadBuffer);
    }

    private getAvailableReadArray(): Uint8Array {
        const readDataLength = this.remainingReadData?.length ?? 0;
        const availableBufferSpace =
            this.backingReadBuffer.byteLength - readDataLength;
        if (availableBufferSpace < HEADER_LENGTH_IN_BYTES) {
            this.increaseReadBufferSize(1024);
        } else if (this.remainingReadData?.byteOffset ?? 0 > 0) {
            this.moveRemainingReadData(this.backingReadBuffer);
        }

        return new Uint8Array(
            this.backingReadBuffer,
            readDataLength,
            this.backingReadBuffer.byteLength - readDataLength
        );
    }

    private async read(): Promise<number> {
        if (!this.socket) {
            return 0;
        }
        const dataArray = this.getAvailableReadArray();
        const [bytesRead, bytesRemaining] = await this.logger.read(
            () =>
                this.socket!!.read(dataArray.byteOffset, dataArray.byteLength),
            ([a, b]: [number, number]) => a
        );
        const bytesToParse = (this.remainingReadData?.length ?? 0) + bytesRead;
        let counter = 0;
        while (counter <= bytesToParse - HEADER_LENGTH_IN_BYTES) {
            const header = new DataView(
                this.backingReadBuffer.buffer,
                counter,
                12
            );
            const length = header.getUint32(0, true);
            if (length === 0) {
                throw new Error("length 0");
            }
            if (counter + length > bytesToParse) {
                this.remainingReadData = new Uint8Array(
                    dataArray.buffer,
                    counter,
                    bytesToParse - counter
                );
                this.increaseReadBufferSize(Math.max(bytesRemaining, length));
                return bytesRead;
            }
            const callbackIndex = header.getUint32(4, true);
            const responseType = header.getUint32(8, true) as ResponseType;
            if (callbackIndex === undefined) {
                throw new Error("Callback is undefined");
            }
            if (this === undefined) {
                throw new Error("this is undefined");
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
                    dataArray.buffer,
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

        if (counter == bytesToParse) {
            this.remainingReadData = undefined;
        } else if (counter > bytesToParse) {
            throw new Error(`${counter} ${bytesToParse}`);
        } else {
            this.remainingReadData = new Uint8Array(
                dataArray.buffer,
                counter,
                bytesToParse - counter
            );
        }

        if (this.backingReadBuffer.byteLength < bytesRemaining) {
            this.increaseReadBufferSize(bytesRemaining);
        }
        return bytesRead;
    }

    private async loopReading() {
        while (this.socket) {
            await this.read();
        }
    }

    private constructor(socket: SocketLikeClient) {
        this.socket = socket;
        this.socket.setReadBuffer(this.backingReadBuffer);
        this.socket.setWriteBuffer(this.backingWriteBuffer);
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
            this.backingWriteBuffer.buffer,
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

    public getPrints(): Record<string, number> {
        return this.logger.getPrints();
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
            this.backingWriteBuffer = new Uint8Array(requiredBufferLength);
            this.socket?.setWriteBuffer(this.backingWriteBuffer);
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
        await this.logger.write(
            () => this.socket!!.write(uint8Array.byteLength),
            cursor
        );
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
