# stream-diff

> Finds the difference between two streams.

```
npm install stream-diff
```

Work similar to the linux `diff` command. Compares two streams, A and B. Returns an object stream that summarizes the changes that need to happen to the items in stream A to make it match the items in stream B.

It can compare two files 2 million records each in about 2 seconds.

Assumes both streams are sorted and that the compare function matches the sorting (i.e. comparing one item in a list to the next item in the list will always show the next item as greater than; comparing an item to itself will show them as equal; etc).

## Example

```js
import fs from 'fs'
import StreamDiff from 'stream-diff'

const fileOne = fs.createReadStream('one.txt')
const fileTwo = fs.createReadStream('two.txt')

const compare = (a, b) => {
    if(a === b) return 0
    else if(a < b) return -1
    else return 1
}

const resultStream = new CompareStreams({
    stream: fileOne,
    lineToRecord: x => parseInt(x),
}, {
    stream: fileTwo,
    lineToRecord: x => parseInt(x),
}, compare)

// this may be called multiple times, as the stream processes
resultStream.on('data', result => {
    console.log(result)
})

resultStream.on('end', () => {
    console.log('all done')
})
```

## API

### StreamDiff(streamOneConfig, streamTwoConfig, compare?)

Takes two streams and compares them. Returns an object stream that emits data that summarizes the changes that need to happen to the items in stream A to make it match the items in stream B.

Each stream config is an object with these properties:

- **stream** *(Stream, required)* — The stream to compare.
- **chunkToLines** *(funciton, optional)* — A function that transforms a chunk from the stream into an array of lines. If not provided, will just split the chunk on newline characters.
- **lineToRecord** *(function, optional)* — A function that transforms a line into a record. If not provided, will just return the line unchanged.

**Returns** `Stream`. On the `data` event, will receive an object with these properties:

- **add** *(array)* — A list of items from stream Two that need to be added to stream One.
- **remove** *(array)* — A list of items from stream One that need to be removed.

### chunkToLines(chunk, partialLine)

This is an optional parameter that can be passed with a stream's config object. It takes a chunk and splits it into lines.

The `partialLine` parameter is used if the stream is coming from a file. Since read streams just read chunks from a file, not line-by-line, there's a chance it will send you data that ends in the middle of a line. In that case, it is useful remove the last line from the stream and wait to diff it until the next chunk arrives, by prepending it to the beginning of the chunk. That way, if the previous chunk did split on the line, it will be processed with its complete line on receiving the next chunk.

**Returns** `object` with these properties:

- **partialLine** *(any)* — The partialLine from this chunk (will be passed on the next call to `chunkToLines`).
- **lines** *(array)* — The raw lines. These do not have to be records. Records can be obtained by passing the `lineToRecord` property to the config.

An example implementation for working with text files, splitting at new lines (this is the default implementation):

```js
const chunkToLines = (chunk, partialLine) => {
    chunk = chunk.toString()

    // prepend the partial line in case it wasn't complete
    if(partialLine) {
        // don't inlcude a '\n' between these because partialLine is part of the first record
        chunk = partialLine + chunk
    }

    const lines = chunk.split('\n')
    const nextPartialLine = lines.splice(lines.length - 1, 1)[0]

    return {
        lines,
        partialLine: nextPartialLine,
    }
}
```

### lineToRecord(line)

Transforms a line into a record. For example, if the stream is a CSV file, this could transform a single CSV line into a record object.

- **line** *(any)* — A single line returned from the `chunkToLines` function. By default this is a string.

**Returns** anything you want. By default, it will return a string.

## Full example streaming from two files containing a list of numbers

```js
import fs from 'fs'
import StreamDiff from 'stream-diff'

const fileOne = fs.createReadStream('one.txt')
const fileTwo = fs.createReadStream('two.txt')

const compare = (a, b) => {
    if(a === b) return 0
    else if(a < b) return -1
    else return 1
}

const lineToRecord = line => parseInt(line)

const chunkToLines = (chunk, partialLine) => {
    chunk = chunk.toString()

    // prepend the partial line in case it wasn't complete
    if(partialLine) {
        // don't inlcude a '\n' between these because partialLine is part of the first record
        chunk = partialLine + chunk
    }

    const lines = chunk.split('\n')
    const nextPartialLine = lines.splice(lines.length - 1, 1)[0]

    return {
        lines,
        partialLine: nextPartialLine,
    }
}

const resultStream = new CompareStreams({
    stream: fileOne,
    lineToRecord,
    chunkToLines,
}, {
    stream: fileTwo,
    lineToRecord,
    chunkToLines,
}, compare)

resultStream.on('data', data => {
    console.log(data)
})

resultStream.on('end', () => {
    console.log('all done')
})
```

## Full example streaming from arrays

```js
import fs from 'fs'
import StreamDiff from 'stream-diff'
import {Readable} from 'stream'

const StreamFromArray = arr => {
    const stream = new Readable({objectMode: true})

    setTimeout(() => {
        arr.forEach(x => stream.push(x))
    }, 0)

    return stream
}

const streamOne = StreamFromArray([{num: 1}, {num: 30}, {num: 50}])
const streamTwo = StreamFromArray([{num: 1}, {num: 20}, {num: 60}])

const compare = (a, b) => {
    if(a.num === b.num) return 0
    else if(a.num < b.num) return -1
    else return 1
}

const lineToRecord = line => line

// don't need to use partialLine here
const chunkToLines = (chunk) => {
    return {
        lines: [chunk], // chunk is an item from our array
        partialLine: null, // could be anything, we're not using it
    }
}

const resultStream = new CompareStreams({
    stream: streamOne,
    lineToRecord,
    chunkToLines,
}, {
    stream: streamTwo,
    lineToRecord,
    chunkToLines,
}, compare)

resultStream.on('data', data => {
    console.log(data)
})

resultStream.on('end', () => {
    console.log('all done')
})
```
