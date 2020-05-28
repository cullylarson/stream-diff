import fs from 'fs'
import path from 'path'
import {Readable} from 'stream'
import CompareStreams from '../esm/'

test('Compares two short files with numbers correctly.', () => {
    const resultCombined = {
        add: [],
        remove: [],
    }

    return new Promise((resolve, reject) => {
        const fileOne = fs.createReadStream(path.join(__dirname, 'files/short-numbers-1.txt'), {highWaterMark: 50})
        const fileTwo = fs.createReadStream(path.join(__dirname, 'files/short-numbers-2.txt'), {highWaterMark: 30})

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

        resultStream.on('data', data => {
            resultCombined.add = [...resultCombined.add, ...data.add]
            resultCombined.remove = [...resultCombined.remove, ...data.remove]
        })

        resultStream.on('error', err => {
            reject(err)
        })

        resultStream.on('end', () => {
            expect(resultCombined.add).toEqual([6, 8, 18, 37])
            expect(resultCombined.remove).toEqual([5, 7, 17, 36, 46, 56, 57, 58, 59, 61, 62, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 101, 102, 103, 104, 105, 106, 107, 108])
            resolve()
        })
    })
})

test.only('Compares two array streams correctly.', () => {
    const resultCombined = {
        add: [],
        remove: [],
    }

    return new Promise((resolve, reject) => {
        const StreamFromArray = l => arr => {
            const arrCopy = [...arr]

            return new Readable({
                objectMode: true,

                read() {
                    let item
                    console.log('pushing')
                    do {
                        item = arrCopy.shift()

                        if(!item) {
                            console.log(`${l} / ending`)
                            this.push(null)
                            return
                        }

                        console.log(`${l} / pushing item`)
                    }
                    while(this.push(item))
                    console.log('done pushing')
                },
            })
        }

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

        const streamOne = StreamFromArray('one')([{num: 1}, {num: 30}, {num: 50}])
        const streamTwo = StreamFromArray('two')([{num: 1}, {num: 20}, {num: 60}])

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
            resultCombined.add = [...resultCombined.add, ...data.add]
            resultCombined.remove = [...resultCombined.remove, ...data.remove]
        })

        resultStream.on('error', err => {
            reject(err)
        })

        resultStream.on('end', () => {
            expect(resultCombined.add).toEqual([])
            expect(resultCombined.remove).toEqual([])
            resolve()
        })
    })
})
