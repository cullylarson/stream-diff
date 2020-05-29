import fs from 'fs'
import path from 'path'
import {Readable} from 'stream'
import StreamDiff from '../esm/'

const bufferToStream = buffer => {
    const stream = new Readable()
    stream.push(buffer)
    stream.push(null)
    return stream
}

const slowStreamFromArray = arr => {
    // [min, max)
    const randInt = (min, max) => {
        return Math.random() * (max - min) + min
    }

    const pushOnTimeout = (timeout, i, stream) => {
        setTimeout(() => {
            if(i === arr.length) {
                stream.push(null)
            }
            else {
                stream.push(arr[i])
                pushOnTimeout(timeout + (randInt(10, 40)), i + 1, stream)
            }
        }, timeout)
    }

    let first = true

    return new Readable({
        objectMode: true,

        read() {
            if(first) {
                first = false
                pushOnTimeout(randInt(10, 80), 0, this)
            }
        },
    })
}

const compareTwoArrayStreams = (arrOne, arrTwo, expectAdd, expectRemove) => {
    const resultCombined = {
        add: [],
        remove: [],
    }

    return new Promise((resolve, reject) => {
        const compare = (a, b) => {
            if(a.num === b.num) return 0
            else if(a.num < b.num) return -1
            else return 1
        }

        const lineToRecord = x => x

        // don't need to use partialLine here
        const chunkToLines = (chunk) => {
            return {
                lines: [chunk], // chunk is an item from our array
                partialLine: null, // could be anything, we're not using it
            }
        }

        const streamOne = Readable.from(arrOne)
        const streamTwo = Readable.from(arrTwo)

        const resultStream = new StreamDiff({
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
            expect(resultCombined.add).toEqual(expectAdd)
            expect(resultCombined.remove).toEqual(expectRemove)
            resolve()
        })
    })
}

const compareTwoSlowArrayStreams = (arrOne, arrTwo, expectAdd, expectRemove) => {
    const resultCombined = {
        add: [],
        remove: [],
    }

    return new Promise((resolve, reject) => {
        const compare = (a, b) => {
            if(a.num === b.num) return 0
            else if(a.num < b.num) return -1
            else return 1
        }

        const lineToRecord = x => x

        // don't need to use partialLine here
        const chunkToLines = (chunk) => {
            return {
                lines: [chunk], // chunk is an item from our array
                partialLine: null, // could be anything, we're not using it
            }
        }

        const streamOne = slowStreamFromArray(arrOne)
        const streamTwo = slowStreamFromArray(arrTwo)

        const resultStream = new StreamDiff({
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
            expect(resultCombined.add).toEqual(expectAdd)
            expect(resultCombined.remove).toEqual(expectRemove)
            resolve()
        })
    })
}

const compareTwoBufferStreams = (bufferOne, bufferTwo, expectAdd, expectRemove) => {
    const resultCombined = {
        add: [],
        remove: [],
    }

    return new Promise((resolve, reject) => {
        const streamOne = bufferToStream(bufferOne)
        const streamTwo = bufferToStream(bufferTwo)

        const compare = (a, b) => {
            if(a === b) return 0
            else if(a < b) return -1
            else return 1
        }

        const resultStream = new StreamDiff({
            stream: streamOne,
            lineToRecord: x => parseInt(x),
        }, {
            stream: streamTwo,
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
            expect(resultCombined.add).toEqual(expectAdd)
            expect(resultCombined.remove).toEqual(expectRemove)
            resolve()
        })
    })
}

const compareTwoNumberFiles = (pathOne, pathTwo, expectAdd, expectRemove) => {
    const resultCombined = {
        add: [],
        remove: [],
    }

    return new Promise((resolve, reject) => {
        const fileOne = fs.createReadStream(path.join(__dirname, pathOne), {highWaterMark: 50})
        const fileTwo = fs.createReadStream(path.join(__dirname, pathTwo), {highWaterMark: 30})

        const compare = (a, b) => {
            if(a === b) return 0
            else if(a < b) return -1
            else return 1
        }

        const resultStream = new StreamDiff({
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
            expect(resultCombined.add).toEqual(expectAdd)
            expect(resultCombined.remove).toEqual(expectRemove)
            resolve()
        })
    })
}

test('Compares two short files with numbers correctly.', () => {
    return compareTwoNumberFiles(
        'files/short-numbers-1.txt',
        'files/short-numbers-2.txt',
        [6, 8, 18, 37, 47, 110],
        [5, 7, 17, 36, 46, 56, 57, 58, 59, 61, 62, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 101, 102, 103, 104, 105, 106, 107, 108],
    )
})

test('Compares two short files with numbers correctly (2).', () => {
    return compareTwoNumberFiles(
        'files/more-short-numbers-1.txt',
        'files/more-short-numbers-2.txt',
        [6, 8, 18],
        [5, 7, 17],
    )
})

test('Compares two array streams correctly.', () => {
    return compareTwoArrayStreams(
        [{num: 1}, {num: 30}, {num: 50}],
        [{num: 1}, {num: 20}, {num: 60}],
        [{num: 20}, {num: 60}],
        [{num: 30}, {num: 50}],
    )
})

test('Compares two array streams correctly (2).', () => {
    return compareTwoArrayStreams(
        [{num: 2}, {num: 20}, {num: 25}, {num: 30}],
        [{num: 1}, {num: 30}, {num: 40}],
        [{num: 1}, {num: 40}],
        [{num: 2}, {num: 20}, {num: 25}],
    )
})

test('Compares two array streams correctly (3).', () => {
    return compareTwoArrayStreams(
        [{num: 1}, {num: 20}, {num: 60}],
        [{num: 1}, {num: 30}, {num: 50}],
        [{num: 30}, {num: 50}],
        [{num: 20}, {num: 60}],
    )
})

test("The 'add' values all come from stream Two, and the 'remove' values all come form stream One", () => {
    return compareTwoArrayStreams(
        [{num: 1, name: 'ONE'}, {num: 30, name: 'ONE'}, {num: 50, name: 'ONE'}],
        [{num: 1, name: 'TWO'}, {num: 20, name: 'TWO'}, {num: 60, name: 'TWO'}],
        [{num: 20, name: 'TWO'}, {num: 60, name: 'TWO'}],
        [{num: 30, name: 'ONE'}, {num: 50, name: 'ONE'}],
    )
})

test('Compares two slow array streams correctly.', () => {
    return compareTwoSlowArrayStreams(
        [{num: 1}, {num: 30}, {num: 50}],
        [{num: 1}, {num: 20}, {num: 60}],
        [{num: 20}, {num: 60}],
        [{num: 30}, {num: 50}],
    )
})

test('Compares two slow array streams correctly (2).', () => {
    return compareTwoSlowArrayStreams(
        [{num: 2}, {num: 20}, {num: 25}, {num: 30}],
        [{num: 1}, {num: 30}, {num: 40}],
        [{num: 1}, {num: 40}],
        [{num: 2}, {num: 20}, {num: 25}],
    )
})

test('Compares two slow array streams correctly (3).', () => {
    return compareTwoSlowArrayStreams(
        [{num: 1}, {num: 20}, {num: 60}],
        [{num: 1}, {num: 30}, {num: 50}],
        [{num: 30}, {num: 50}],
        [{num: 20}, {num: 60}],
    )
})

test('Compares two buffer streams correctly.', () => {
    return compareTwoBufferStreams(
        Buffer.from('2\n20\n25\n30', 'utf8'),
        Buffer.from('1\n30\n40', 'utf8'),
        [1, 40],
        [2, 20, 25],
    )
})

test('Compares two buffer streams correctly (2).', () => {
    return compareTwoBufferStreams(
        Buffer.from('2\n3\n5\n20\n60', 'utf8'),
        Buffer.from('1\n5\n30\n50', 'utf8'),
        [1, 30, 50],
        [2, 3, 20, 60],
    )
})
