import {Readable} from 'stream'

// TODO -- process partialLine as its own record before ending

const compareDefault = (a, b) => {
    if(a === b) return 0
    else if(a < b) return -1
    else return 1
}

const debug = false

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
            records: [],
            partialLine: '',
            readable: false,
        }

        this.b = {
            name: 'b',
            stream: bInfo.stream,
            lineToRecord: bInfo.lineToRecord || lineToRecordDefault,
            chunkToLines: bInfo.chunkToLines || chunkToLinesDefault,
            records: [],
            partialLine: '',
            readable: false,
        }

        this.a.stream.pause()
        this.b.stream.pause()
        this.pause()

        // when both are readable, resume
        this.a.stream.once('readable', () => {
            this.a.readable = true
            if(this.b.readable) this.resume()
        })

        this.b.stream.once('readable', () => {
            this.b.readable = true
            if(this.a.readable) this.resume()
        })
    }

    _read() {
        // TODO -- turn into package
        // TODO -- test

        const FAULT = Symbol('error')
        const EMPTY = Symbol('empty')
        const SUCCESS = Symbol('success')

        const readData = (streamInfo) => {
            debug && console.log(`${streamInfo.name} / readData`)

            if(!streamInfo.stream.readable) {
                return SUCCESS
            }

            let chunk
            try {
                chunk = streamInfo.stream.read()
                debug && console.log(`${streamInfo.name} / `, {chunk})
            }
            catch(err) {
                debug && console.log(`${streamInfo.name} / FAULT`)
                this.destroy(err)
                return FAULT
            }

            if(!chunk) {
                debug && console.log(`${streamInfo.name} / empty chunk`)

                return EMPTY   
            }

            const {lines, partialLine} = streamInfo.chunkToLines(chunk, streamInfo.partialLine)
            const records = lines.map(streamInfo.lineToRecord)

            streamInfo.records = [...streamInfo.records, ...records]
            streamInfo.partialLine = partialLine

            debug && console.log(`${streamInfo.name} / read success`)

            return SUCCESS
        }

        const processData = () => {
            // A is empty, everyting in B is not in A's set, so needs to be added
            if(!this.a.stream.readable && this.a.records.length === 0) {
                debug && console.log('process / a ended')

                const addRecords = this.b.records
                this.b.records = []

                return {
                    add: addRecords,
                    remove: [],
                }
            }
            // B is empty, everything in A is not in B's set, so needs to be removed
            else if(!this.b.stream.readable && this.b.records.length === 0) {
                debug && console.log('process / b ended')

                const removeRecords = this.a.records
                this.a.records = []

                return {
                    add: [],
                    remove: removeRecords,
                }
            }
            else {
                debug && console.log('process / both active')

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

                return result
            }
        }

        debug && console.log('_read')

        this.a.stream.resume()
        this.b.stream.resume()

        let result
        do {
            const readResultA = readData(this.a)
            // fatal
            if(readResultA === FAULT) break
            // stream isn't producing, need to wait until there's more data
            if(readResultA === EMPTY) break

            const readResultB = readData(this.b)
            // fatal
            if(readResultB === FAULT) break
            // stream isn't producing, need to wait until there's more data
            if(readResultB === EMPTY) break

            if(!this.a.stream.readable && !this.b.stream.readable) {
                debug && console.log('all done')
                this.push(null)
                break
            }

            result = processData()
            debug && console.log({result: JSON.stringify(result)})
        } while(this.push(result))

        this.a.stream.pause()
        this.b.stream.pause()

        return

        debug && console.log('read triggered')
        let done = false // done is true when data has been pushed to the stream

        // creates an 'end' event listern for one of the streams
        const HandleEnd = (thisStreamInfo, otherStreamInfo, handleData, handleEnd, handleError) => {
            console.log(`${thisStreamInfo.name} end`)
            thisStreamInfo.state = 'end'
            if(thisStreamInfo.name === 'a') console.log(`${thisStreamInfo.name} / off / data / 001`) // stub
            if(thisStreamInfo.name === 'a') console.log(`${thisStreamInfo.name} / off / end / 001`) // stub
            thisStreamInfo.stream.removeListener('data', handleData)
            thisStreamInfo.stream.removeListener('end', handleEnd)
            thisStreamInfo.stream.removeListener('error', handleError)
            if(thisStreamInfo.name === 'a') console.log(`${thisStreamInfo.name} counts / data: ${thisStreamInfo.stream.listenerCount('data')}, error: ${thisStreamInfo.stream.listenerCount('error')}, end: ${thisStreamInfo.stream.listenerCount('end')}`) // stub

            // if both streams have ended
            if(otherStreamInfo.state === 'end') this.push(null)
        }

        // creates an 'data' event listern for one of the streams
        const HandleData = (streamInfo, handleData, chunk) => {
            const {lines, partialLine} = streamInfo.chunkToLines(chunk, streamInfo.partialLine)
            console.log(`${streamInfo.name} data`, {lines, partialLine})

            const records = lines.map(streamInfo.lineToRecord)

            streamInfo.records = [...streamInfo.records, ...records]
            streamInfo.partialLine = partialLine

            if(streamInfo.name === 'a') console.log(`${streamInfo.name} / off / data / 002`) // stub
            streamInfo.stream.removeListener('data', handleData)
            if(streamInfo.name === 'a') console.log(`${streamInfo.name} counts / data: ${streamInfo.stream.listenerCount('data')}, error: ${streamInfo.stream.listenerCount('error')}, end: ${streamInfo.stream.listenerCount('end')}`) // stub
            handleAnyData()
            if(!done && streamInfo.state === 'ready') {
                if(streamInfo.name === 'a') console.log(`${streamInfo.name} / on / data / 001`)
                streamInfo.stream.on('data', handleData)
            }
        }

        const cleanup = () => {
            debug && console.log('cleanup')
            done = true

            console.log('a / off / data / 003') // stub
            this.a.stream.removeListener('data', handleDataA)
            this.a.stream.removeListener('end', handleEndA)
            console.log('a / off / end / 002') // stub
            this.a.stream.removeListener('error', handleError)
            console.log(`a counts / data: ${this.a.stream.listenerCount('data')}, error: ${this.a.stream.listenerCount('error')}, end: ${this.a.stream.listenerCount('end')}`) // stub

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

                cleanup()
            }
            // B is empty, everything in A is not in B's set, so needs to be removed
            else if(this.b.state === 'end' && this.b.records.length === 0) {
                this.push({
                    add: [],
                    remove: this.a.records,
                })

                this.a.records = []

                cleanup()
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

                cleanup()
            }
        }

        debug && console.log(`a state: ${this.a.state}, b state: ${this.b.state}`)

        if(this.a.state === 'ready') {
            console.log('A / on / data / 002') // stub
            this.a.stream.on('data', handleDataA)
            this.a.stream.on('error', handleError)
            console.log('A / on / end / 003') // stub
            this.a.stream.on('end', handleEndA)
        }

        if(this.b.state === 'ready') {
            this.b.stream.on('data', handleDataB)
            this.b.stream.on('error', handleError)
            this.b.stream.on('end', handleEndB)
        }
    }
}
