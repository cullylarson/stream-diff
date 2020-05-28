import {Readable} from 'stream'

const compareDefault = (a, b) => {
    if(a === b) return 0
    else if(a < b) return -1
    else return 1
}

const DEBUG = false

const chunkToLinesDefault = (chunk, partialLine, shouldGeneratePartialLine) => {
    chunk = chunk.toString()

    // prepend the partial line in case it wasn't complete
    if(partialLine) {
        // don't inlcude a '\n' between these because partialLine is part of the first record
        chunk = partialLine + chunk
    }

    const lines = chunk.split('\n')
    const nextPartialLine = shouldGeneratePartialLine
        ? lines.splice(lines.length - 1, 1)[0]
        : null

    return {
        lines,
        partialLine: nextPartialLine || null,
    }
}

const lineToRecordDefault = x => x

const isCompletelyDone = streamInfo => streamInfo.state === 'end' && !hasRecords(streamInfo) && !streamInfo.partialLine

const isDoneWithPartial = streamInfo => streamInfo.state === 'end' && streamInfo.partialLine

const hasRecords = streamInfo => streamInfo.records.length !== 0

// both streams are ended, but at least one has a record left or a partialLine, need to resume to process it
const needOneMoreIteration = (aInfo, bInfo) => aInfo.state === 'end' && bInfo.state === 'end' && (hasRecords(aInfo) || hasRecords(bInfo) || aInfo.partialLine || bInfo.partialLine)

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
            partialLine: null,
            state: 'paused',
        }

        this.b = {
            name: 'b',
            stream: bInfo.stream,
            lineToRecord: bInfo.lineToRecord || lineToRecordDefault,
            chunkToLines: bInfo.chunkToLines || chunkToLinesDefault,
            records: [],
            partialLine: null,
            state: 'paused',
        }

        this.a.stream.pause()
        this.b.stream.pause()
        this.pause()

        this.a.stream.once('end', () => {
            DEBUG && console.info('end / A / B done? ', this.b.state, this.b.records.length)

            this.a.state = 'end'

            if(isCompletelyDone(this.a) && isCompletelyDone(this.b)) {
                DEBUG && console.info('all done')
                this.resume()
                this.push(null)
            }
            else if(needOneMoreIteration(this.a, this.b)) {
                DEBUG && console.info('At least one more thing to process /', this.a.records, '/', this.a.partialLine, '/', this.b.records, '/', this.b.partialLine)
                this.resume()
            }
        })
        this.b.stream.once('end', () => {
            DEBUG && console.info('end / B / A done? ', this.a.state, this.a.records.length)

            this.b.state = 'end'

            if(isCompletelyDone(this.a) && isCompletelyDone(this.b)) {
                DEBUG && console.info('all done')
                this.resume()
                this.push(null)
            }
            else if(needOneMoreIteration(this.a, this.b)) {
                DEBUG && console.info('At least one more thing to process /', this.a.records, '/', this.a.partialLine, '/', this.b.records, '/', this.b.partialLine)
                this.resume()
            }
        })

        // when both are readable, resume

        this.a.stream.once('readable', () => {
            this.a.state = 'ready'
            if(this.b.state === 'ready') this.resume()
        })

        this.b.stream.once('readable', () => {
            this.b.state = 'ready'
            if(this.b.state === 'ready') this.resume()
        })
    }

    _read() {
        const FAULT = Symbol('error')
        const EMPTY = Symbol('empty')
        const SUCCESS = Symbol('success')

        const readData = (streamInfo) => {
            DEBUG && console.info(`${streamInfo.name} / readData`)

            let chunk
            let shouldPreserverPartialLine = true

            // we might have a partialLine left to process. if so need to treat it as a chunk
            if(streamInfo.state === 'end' && !streamInfo.partialLine) {
                return SUCCESS
            }
            else if(streamInfo.state === 'end' && streamInfo.partialLine) {
                DEBUG && console.info(`${streamInfo.name} / treating partialLine as chunk /`, streamInfo.partialLine)
                shouldPreserverPartialLine = false
                chunk = streamInfo.partialLine
                streamInfo.partialLine = null
            }
            else {
                try {
                    chunk = streamInfo.stream.read()
                    DEBUG && console.info(`${streamInfo.name} / `, {chunk: Buffer.isBuffer(chunk) ? chunk.toString() : chunk})
                }
                catch(err) {
                    DEBUG && console.info(`${streamInfo.name} / FAULT / `, err)
                    this.destroy(err)
                    return FAULT
                }
            }

            if(!chunk) {
                DEBUG && console.info(`${streamInfo.name} / empty chunk`)

                return EMPTY
            }

            const {lines, partialLine} = streamInfo.chunkToLines(chunk, streamInfo.partialLine, shouldPreserverPartialLine)
            DEBUG && console.info(`${streamInfo.name} / `, {lines, partialLine})
            const records = lines.map(streamInfo.lineToRecord)

            streamInfo.records = [...streamInfo.records, ...records]
            streamInfo.partialLine = partialLine

            DEBUG && console.info(`${streamInfo.name} / read success`)

            return SUCCESS
        }

        const processData = () => {
            let result

            DEBUG && console.info('process', this.a.records, this.b.records)
            // A is done, everything in B is not in A's set, so needs to be added
            if(isCompletelyDone(this.a)) {
                DEBUG && console.info('process / a ended / put all b records on add')

                const addRecords = this.b.records
                this.b.records = []

                result = {
                    add: addRecords,
                    remove: [],
                }
            }
            // B is done, everything in A is not in B's set, so needs to be removed
            else if(isCompletelyDone(this.b)) {
                DEBUG && console.info('process / b ended / put all a records on remove')

                const removeRecords = this.a.records
                this.a.records = []

                result = {
                    add: [],
                    remove: removeRecords,
                }
            }
            else {
                DEBUG && console.info('process / both streams')

                let recordA = null
                let recordB = null

                result = {
                    add: [],
                    remove: [],
                }

                while(hasRecords(this.a) && hasRecords(this.b)) {
                    DEBUG && console.info('compare loop /', this.a.records, '/', this.b.records)
                    recordA = this.a.records.shift()
                    recordB = this.b.records.shift()

                    DEBUG && console.info('compare /', recordA, '/', recordB)
                    const comparison = this.compare(recordA, recordB)

                    // if they're equal, then we don't need to do anything and can continue to the next record from both sets
                    if(comparison === 0) {
                        DEBUG && console.info('compare result / equal / do nothing')
                        recordA = null
                        recordB = null
                    }
                    // if A < B, then A is not in B's set and needs to be removed
                    else if(comparison === -1) {
                        DEBUG && console.info('compare result / A < B / remove A entry: ', recordA)
                        result.remove.push(recordA)
                        recordA = null
                    }
                    // if A > B, then B is not in A's set and needs to be added
                    else {
                        DEBUG && console.info('compare result / A > B / add B entry: ', recordB)
                        result.add.push(recordB)
                        recordB = null
                    }

                    // put the used record back on so it can be consumed on the next iteration (this is simpler than having to check the records array and the used record to see if we need to continue looping)
                    if(recordA !== null) {
                        this.a.records.unshift(recordA)
                    }
                    if(recordB !== null) {
                        this.b.records.unshift(recordB)
                    }

                    DEBUG && console.info('after compare /', recordA, '/', recordB)
                }
            }

            return result
        }

        const Resume = (thisStreamInfo, otherStreamInfo) => () => {
            DEBUG && console.info(`resume / ${thisStreamInfo.name}`)

            if(thisStreamInfo.state !== 'end') {
                thisStreamInfo.state = 'ready'
            }

            // wait for both to be ready so we aren't just reading one stream over and over while the other is stalled for some reason (i.e. waiting for more data to send)
            if(otherStreamInfo.state === 'end' || otherStreamInfo.state === 'ready') {
                DEBUG && console.info('resume / both streams ready')
                this.resume()
            }

            DEBUG && console.info(`after resume / ${thisStreamInfo.state} / ${otherStreamInfo.state}`)
        }

        DEBUG && console.info('_read')

        this.a.stream.resume()
        this.b.stream.resume()

        let result
        do {
            if(!hasRecords(this.a) || isDoneWithPartial(this.a)) {
                const readResult = readData(this.a)
                // fatal
                if(readResult === FAULT) break
            }

            if(!hasRecords(this.b) || isDoneWithPartial(this.b)) {
                const readResult = readData(this.b)
                // fatal
                if(readResult === FAULT) break
            }

            if(isCompletelyDone(this.a) && isCompletelyDone(this.b)) {
                DEBUG && console.info('both streams completely done')
                break
            }
            /*
            */

            result = processData()
            DEBUG && console.info({result: JSON.stringify(result)})
        } while(this.push(result))

        DEBUG && console.info('pausing everything')

        if(isCompletelyDone(this.a) && isCompletelyDone(this.b)) {
            DEBUG && console.info('all done')
            DEBUG && console.info('partial line? /', this.a.partialLine, this.b.partialLine)
            this.push(null)
            return
        }

        this.a.stream.pause()
        this.b.stream.pause()

        // don't pause if both the streams are ended and we have more data, since we need
        // one more iteration to finish

        if (
            this.a.state === 'end'
            && this.b.state === 'end'
            && (hasRecords(this.a) || hasRecords(this.b))
        ) {
            // don't pause. this conditional logic made more sense that not'ing it.
        }
        else {
            this.pause()
        }

        if(this.a.state !== 'end') this.a.state = 'paused'
        if(this.b.state !== 'end') this.b.state = 'paused'

        if(this.a.state !== 'end') this.a.stream.once('readable', Resume(this.a, this.b))
        if(this.b.state !== 'end') this.b.stream.once('readable', Resume(this.b, this.a))
    }
}
