import { createHash } from 'crypto'
import { readdirSync, readFileSync, statSync } from 'fs'
import { join } from 'path'
import DB from 'better-sqlite3-helper'
import { insertChunk } from './db'
import {
  insertContent,
  insertFile,
  insertOrReuseChunk,
  ChunkType,
  updateChunkContent,
  db,
  hasFile,
  findChunkByHash,
} from './db'

let hashAlgo = 'sha256'
let hashLength = 256 / 8
let minChunkLength = hashLength * 4

export function hashContent(content: Buffer) {
  let hash = createHash(hashAlgo)
  hash.write(content)
  return hash.digest()
}

let treeContentPlaceHolder = Buffer.from([])

function storeContentPart(content: Buffer, contentHash: Buffer) {
  let chunk_id = findChunkByHash(contentHash)
  if (chunk_id) {
    return chunk_id
  }
  if (content.length <= minChunkLength) {
    return insertChunk({
      hash: contentHash,
      content,
      type: ChunkType.Raw,
    })
  }

  let mid = Math.floor(content.length / 2)

  let leftContent = content.slice(0, mid)
  let leftHash = hashContent(leftContent)

  let rightContent = content.slice(mid)
  let rightHash = hashContent(rightContent)

  let treeChunkId = insertChunk({
    hash: contentHash,
    content: treeContentPlaceHolder,
    type: ChunkType.Tree,
  })

  let leftChunkId = storeContentPart(leftContent, leftHash)
  let rightChunkId = storeContentPart(rightContent, rightHash)

  let treeContent = leftChunkId.toString(36) + '+' + rightChunkId.toString(36)
  updateChunkContent(treeChunkId, Buffer.from(treeContent))

  return treeChunkId
}

export function storeContent(content: Buffer) {
  let contentHash = hashContent(content)
  let chunk_id = storeContentPart(content, contentHash)
  return insertContent({ chunk_id, size: content.length })
}

export function storeFile(file: string) {
  let content = readFileSync(file)
  let content_id = storeContent(content)
  return insertFile({ filename: file, content_id })
}

export function scanDir(dir: string) {
  readdirSync(dir).forEach(file => {
    file = join(dir, file)
    let stat = statSync(file)
    if (stat.isDirectory()) {
      scanDir(file)
    } else if (stat.isFile() && !hasFile(file)) {
      storeFile(file)
    }
  })
}

export function report() {
  let analysis_sql = /* sql */ `
select sum(length(content)), type from chunk
group by type
`
  return db.prepare(analysis_sql).all()
}

export function test() {
  // storeFile('raw')
  scanDir('node_modules')
  console.log(report())
}
test()
