import {Readable} from 'stream'

// TODO -- process partialLine as its own record before ending

const compareDefault = (a, b) => {
    if(a === b) return 0
    else if(a < b) return -1
    else return 1
}

const debug = true

const chunkToLinesDefault = (chunk, partialLine) => {
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

const lineToRecordDefault = x => x

export default class CompareStreams extends Readable {
    constructor(aInfo, bInfo, compare) {
        super({objectMode: true})

        this.compare = compare || compareDefault

        this.a = {
            name: 'a',
            stream: aInfo.stream,
            lineToRecord: aInfo.lineToRecord || lineToRecordDefault,
            chunkToLines: aInfo.chunkToLines || chunkToLinesDefault,
            state: 'ready',
            records: [],
            partialLine: '',
        }

        this.b = {
            name: 'b',
            stream: bInfo.stream,
            lineToRecord: bInfo.lineToRecord || lineToRecordDefault,
            chunkToLines: bInfo.chunkToLines || chunkToLinesDefault,
            state: 'ready',
            records: [],
            partialLine: '',
        }
    }

    _read() {
        // TODO -- turn into package
        // TODO -- test

        debug && console.log('read triggered')
        let done = false // done is true when data has been pushed to the stream

        // creates an 'end' event listern for one of the streams
        const HandleEnd = (thisStreamInfo, otherStreamInfo, handleData, handleEnd, handleError) => {
            debug && console.log(`${thisStreamInfo.name} end`)
            thisStreamInfo.state = 'end'
            if(thisStreamInfo.name === 'a') console.log(`${thisStreamInfo.name} / off / data`) // stub
            if(thisStreamInfo.name === 'a') console.log(`${thisStreamInfo.name} / off / end / 001`) // stub
            thisStreamInfo.stream.removeListener('data', handleData)
            thisStreamInfo.stream.removeListener('end', handleEnd)
            thisStreamInfo.stream.removeListener('error', handleError)

            // if both streams have ended
            if(otherStreamInfo.state === 'end') this.push(null)
        }

        // creates an 'data' event listern for one of the streams
        const HandleData = (streamInfo, handleData, chunk) => {
            debug && console.log(`${streamInfo.name} data`, {chunk})
            const {lines, partialLine} = streamInfo.chunkToLines(chunk, streamInfo.partialLine)

            const records = lines.map(streamInfo.lineToRecord)

            streamInfo.records = [...streamInfo.records, ...records]
            streamInfo.partialLine = partialLine

            if(streamInfo.name === 'a') console.log(`${streamInfo.name} / off / data`) // stub
            streamInfo.stream.removeListener('data', handleData)
            handleAnyData()
            if(!done && streamInfo.state === 'ready') {
                if(streamInfo.name === 'a') console.log(`${streamInfo.name} / on / data`)
                streamInfo.stream.on('data', handleData)
            }
        }

        const cleanup = () => {
            debug && console.log('cleanup')
            done = true

            console.log('a / off / data') // stub
            this.a.stream.removeListener('data', handleDataA)
            this.a.stream.removeListener('end', handleEndA)
            console.log('A / off / end / 002') // stub
            this.a.stream.removeListener('error', handleError)

            this.b.stream.removeListener('data', handleDataB)
            this.b.stream.removeListener('end', handleEndB)
            this.b.stream.removeListener('error', handleError)
        }

        const handleError = err => {
            cleanup()
            this.destroy(err)
        }

        const handleEndA = () => HandleEnd(this.a, this.b, handleDataA, handleEndA, handleError)
        const handleEndB = () => HandleEnd(this.b, this.a, handleDataB, handleEndB, handleError)

        const handleDataA = chunk => HandleData(this.a, handleDataA, chunk)
        const handleDataB = chunk => HandleData(this.b, handleDataB, chunk)

        const handleAnyData = () => {
            debug && console.log({haveDataA: Boolean(this.a.records.length), haveDataB: Boolean(this.b.records.length)})

            // if a stream has ended, we won't be getting any more data from it, so just continue with one stream. Otherwise, wait until we have both datasets.
            if((!this.a.records.length && this.a.state !== 'end') || (!this.b.records.length && this.b.state !== 'end')) return

            debug && console.log('have both data', {a: this.a.records, b: this.b.records})

            // A is empty, everyting in B is not in A's set, so needs to be added
            if(this.a.state === 'end' && this.a.records.length === 0) {
                this.push({
                    add: this.b.records,
                    remove: [],
                })

                this.b.records = []
            }
            // B is empty, everything in A is not in B's set, so needs to be removed
            else if(this.b.state === 'end' && this.b.records.length === 0) {
                this.push({
                    add: [],
                    remove: this.a.records,
                })

                this.a.records = []
            }
            else {
                const result = {
                    add: [],
                    remove: [],
                }

                let recordA = null
                let recordB = null

                while(this.a.records.length && this.b.records.length) {
                    if(recordA === null) {
                        recordA = this.a.records.shift()
                    }

                    if(recordB === null) {
                        recordB = this.b.records.shift()
                    }

                    const comparison = this.compare(recordA, recordB)

                    // if they're equal, then we don't need to do anything and can continue to the next record from both sets
                    if(comparison === 0) {
                        recordA = null
                        recordB = null
                    }
                    // if A < b, then A is not in B's set and needs to be removed
                    else if(comparison === -1) {
                        result.remove.push(recordA)
                        recordA = null
                    }
                    // if A > b, then B is not in A's set and needs to be added
                    else {
                        result.add.push(recordB)
                        recordB = null
                    }
                }

                // don't need to clear records because they've either been clearn by `shift`ing them, or have some left for next iteration

                this.push(result)
            }

            cleanup()
        }

        debug && console.log(`a state: ${this.a.state}, b state: ${this.b.state}`)

        if(this.a.state === 'ready') {
            console.log('A / on / data') // stub
            this.a.stream.on('data', handleDataA)
            this.a.stream.on('error', handleError)
            console.log('A / on / end') // stub
            this.a.stream.on('end', handleEndA)
        }

        if(this.b.state === 'ready') {
            this.b.stream.on('data', handleDataB)
            this.b.stream.on('error', handleError)
            this.b.stream.on('end', handleEndB)
        }
    }
}
