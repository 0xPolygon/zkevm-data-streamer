# zkEVM DataStreamer

![zkEVM DataSreamer diagram](doc/data-streamer.png)

## Binary file format

### Header page

The first page is the header.
Header page size = 4096 bytes

#### Magic numbers
At the beginning of the file there are the following magic bytes (file signature): `polygonDATSTREAM`

#### Header entry format

>u8 packetType = 1
>u32 headerLength = 29
>u64 streamType // 1:Sequencer
>u64 totalLength // Total bytes used in the file
>u64 totalEntries // Total number of data entries

### Data page

From the second page starts the data pages.
Page size = 1 MB

#### Data entry format
>u8 packetType // 2:Data entry, 0:Padding, (1:Header)
>u32 length // Total length of the entry (17 bytes + length(data))
>u32 entryType // 1:L2 block, 2:L2 tx
>u64 entryNumber // Entry number (sequential starting with 0)
>u8[] data

If an entry do not fits in the remaining page space we store this entry in the next page.

### Diagram

![Alt](doc/data-streamer-bin-file.drawio.png)

## TCP/IP Commands

All the commands available for stream clients returns a result.
Some commands like start may return more data.

### Start

>u64 command = 1;
>u64 streamType // 1:Sequencer
>u64 fromEntryNumber;

After the command, the system start stream data from that entry number.

If already started terminate connection.

### Stop

>u64 command = 2;
>u64 streamType // 1:Sequencer

Stop streaming data.

If not started terminate connection.

The result is sended just after it's stopped.

### Header

>u64 command = 3;
>u64 streamType // 1:Sequencer

Returns the current status of the header.

If started, terminate the connection.

#### Result format

>u8 packetType // 0xFF:Result
>u32 length // Total length of the entry
>u32 errorNum // Error code (0:OK)
>u8[] errorStr

## Sequencer entries

### Start L2 Block

Entry type = 1

Entry data:
>u64 batchNum  // this field could also be placed in 'batch' section
>u64 blockL2Num // this could be set to u32 since 2**32 - 1 should be enough
>u64 timestamp  // Could be set to u32 since it will have an overflow in 82 years. Just to save 0's
>u8[32] globalExitRoot
>u8[20] coinBase // coinbase is shared across all the batch (maybe add a batch section could be useful to save data)
>u16 forkId // forkId should be u32 according to the snark input. This value is not thought to be changed continously, so it can placed into a different section such as the batch

### L2 TX

Entry type = 2

Entry data:
>u8   gasPricePercentage  // this field could be added into the encodedTX and depending on the forkId field it could be present or not
>u8   isValid  // Intrinsic   // I think intrinsic transaction should not appear here
>u32  encodedTXLength
>u8[] encodedTX

### End L2 Block

Entry type = 3
>u32  l2BlockHash // this should be u8[32]
>u32  stateRoot // this should be u8[32]

## API Interface

StartAtomicOp()
AddStreamEntry(u32 entryType, u8[] data) -> return u64 entryNumber
CommitAtomicOp()
RollbackAtomicOp()
