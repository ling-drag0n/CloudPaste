import { ArchiveCompression, ArchiveFormat } from "./formats.js";
import { ArchiveReader } from "./archive-reader.js";
export { ArchiveCompression, ArchiveFormat } from "./formats.js";
export type ArchiveOptions = {
    workerUrl?: string | URL;
    getWorker?: Function;
    createClient?: (worker: any) => any;
};
export type ArchiveEntryFile = {
    file: ArchiveEntryFile;
    pathname?: string;
};
export type ArchiveWriteOptions = {
    files: ArchiveEntryFile[];
    outputFileName: string;
    compression: ArchiveCompression;
    format: ArchiveFormat;
    passphrase: string | null;
};
export declare class Archive {
    private static _options;
    /**
     * Initialize libarchivejs
     * @param {Object} options
     */
    static init(options?: ArchiveOptions | null): ArchiveOptions;
    static open(file: File): Promise<ArchiveReader>;
    static write({ files, outputFileName, compression, format, passphrase, }: ArchiveWriteOptions): Promise<File>;
    private static getWorker;
    private static getClient;
}
