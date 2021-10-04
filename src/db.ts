import DB from 'better-sqlite3-helper'
import { Int, toSafeMode } from 'better-sqlite3-schema'
import { join } from 'path'

export type NewRow<T> = Omit<T, 'id'>

export const db = DB({
  path: join('data', 'sqlite3.db'),
  migrate: false,
  // migrate: {
  //   migrationsPath: 'migrations',
  //   table: 'migrations',
  //   force: false,
  // },
})

toSafeMode(db)

export enum ChunkType {
  Raw = 'r',
  Tree = 't',
}

export type Chunk = {
  id: Int
  hash: Buffer
  type: ChunkType
  content: Buffer
}

db.exec(/* sql */ `
create table if not exists chunk (
  id integer primary key
, hash blob not null
, type text not null
, content blob not null
);
create unique index if not exists chunk_uniq_idx on chunk(hash);
`)

let select_chunk_id_by_hash = db.prepare(/* sql */ `
select id from chunk
where hash = ?
`)

let insert_chunk = db.prepare(/* sql */ `
insert into chunk
(hash, type, content)
values
(:hash, :type, :content)
`)

let update_chunk_content = db.prepare(/* sql */ `
update chunk
set content = :content
where id = :id
`)

let cleanup_chunk = db.prepare(/* sql */ `
delete from chunk
where type = 't'
  and length(content) = 0
`)

cleanup_chunk.run()

export function findChunkByHash(hash: Buffer): number | undefined {
  return select_chunk_id_by_hash.get(hash)?.id
}

export function insertChunk(chunk: NewRow<Chunk>) {
  return insert_chunk.run(chunk).lastInsertRowid
}

export function insertOrReuseChunk(chunk: NewRow<Chunk>) {
  return findChunkByHash(chunk.hash) || insertChunk(chunk)
}

export function updateChunkContent(id: Int, content: Buffer) {
  update_chunk_content.run({ id, content })
}

export type Content = {
  id: Int
  chunk_id: Int
  size: number
}

db.exec(/* sql */ `
create table if not exists content (
  id integer primary key
, chunk_id integer not null references chunk(id)
, size integer not null
);
`)

let insert_content = db.prepare(`
insert into content
(chunk_id, size)
values
(:chunk_id, :size)
`)

export function insertContent(row: NewRow<Content>) {
  return insert_content.run(row).lastInsertRowid
}

export type File = {
  id: Int
  filename: string
  content_id: Int
}

db.exec(/* sql */ `
create table if not exists file (
  id integer primary key
, filename text not null
, content_id integer references content(id)
);
`)

let insert_file = db.prepare(`
insert into file
(filename, content_id)
values
(:filename, :content_id)
`)

let select_count_file = db.prepare(/* sql */ `
select count(*) as count
from file
where filename = ?
`)

export function insertFile(row: NewRow<File>) {
  return insert_file.run(row).lastInsertRowid
}

export function hasFile(filename: string) {
  return select_count_file.get(filename).count != 0
}
