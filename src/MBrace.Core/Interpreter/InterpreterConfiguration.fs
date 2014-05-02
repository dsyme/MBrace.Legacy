namespace Nessos.MBrace.Core

    open Nessos.MBrace.Utils
    open Nessos.MBrace.Runtime

    type InterpreterConfiguration =
        {
            CloudSeqStore         : Lazy<ICloudSeqStore            >
            CloudRefStore         : Lazy<ICloudRefStore            >
            CloudFileStore        : Lazy<ICloudFileStore           >
            MutableCloudRefStore  : Lazy<IMutableCloudRefStore     >
            LogStore              : Lazy<ILogStore                 >
            Logger                : Lazy<ILogger                   >
            Serializer            : Lazy<Nessos.FsPickler.FsPickler>
        }