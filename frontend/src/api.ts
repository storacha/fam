import { parse as parseDID, decode as decodeDID, from as principalFrom } from '@ipld/dag-ucan/did'
import { DID, Delegation, Result, Signer } from '@ucanto/interface'
import { Link, UnknownLink, Version } from 'multiformats'
import { decode as decodeLink } from 'multiformats/link'
import { ok, error } from '@ucanto/core'
import { ed25519 } from '@ucanto/principal'
import { extract as extractDelegation } from '@ucanto/core/delegation'
import { parse as parseJSON, stringify as encodeJSON } from '@ipld/dag-json'
import { ID, Buckets, AddBucket, Root, Entries, Put, ShareBucket } from '../wailsjs/go/main/App'
import { BrowserOpenURL } from '../wailsjs/runtime/runtime'

export interface InvocationFailure extends Error {
  name: 'InvocationFailure'
}

class InvocationError extends Error implements InvocationFailure {
  get name () {
    return 'InvocationFailure' as const
  }
}

export interface EncodeFailure extends Error {
  name: 'EncodeFailure'
}

class EncodeError extends Error implements EncodeFailure {
  get name () {
    return 'EncodeFailure' as const
  }
}

export interface DecodeFailure extends Error {
  name: 'DecodeFailure'
}

class DecodeError extends Error implements DecodeFailure {
  get name () {
    return 'DecodeFailure' as const
  }
}

export const id = async (): Promise<Result<Signer, InvocationFailure|DecodeFailure>> => {
  let res: string
  try {
    res = await ID()
  } catch (err) {
    return error(new InvocationError('failed to invoke API', { cause: err }))
  }

  let data: Uint8Array
  try {
    data = parseJSON(res)
  } catch (err) {
    return error(new DecodeError('failed to parse API response', { cause: err }))
  }

  try {
    const signer = ed25519.decode(data)
    return ok(signer)
  } catch (err) {
    return error(new DecodeError('failed to decode private key', { cause: err }))
  }
}

export const buckets = async (): Promise<Result<Map<DID, Delegation>, InvocationFailure|DecodeFailure>> => {
  let res: string
  try {
    res = await Buckets()
  } catch (err) {
    return error(new InvocationError('failed to invoke API', { cause: err }))
  }

  let data: { [did: string]: Uint8Array }
  try {
    data = parseJSON(res)
  } catch (err) {
    return error(new DecodeError('failed to parse API response', { cause: err }))
  }

  const buckets = new Map<DID, Delegation>()
  for (const [k, v] of Object.entries(data)) {
    const res = await extractDelegation(v)
    if (res.error) {
      return error(new DecodeError('failed to extract delegation', { cause: res.error }))
    }
    buckets.set(parseDID(k).did(), res.ok)
  }
  return ok(buckets)
}

export const addBucket = async (proof: Delegation): Promise<Result<DID, EncodeFailure|InvocationFailure|DecodeError>> => {
  const archive = await proof.archive()
  if (archive.error) {
    return error(new EncodeError('failed to archive delegation', { cause: archive.error }))
  }

  let input: string
  try {
    input = encodeJSON(archive.ok)
  } catch (err) {
    return error(new EncodeError('failed to stringify API parameters', { cause: err }))
  }

  let res: string
  try {
    res = await AddBucket(input)
  } catch (err) {
    return error(new InvocationError('failed to invoke API', { cause: err }))
  }

  let data: Uint8Array
  try {
    data = parseJSON(res)
  } catch (err) {
    return error(new DecodeError('failed to parse API response', { cause: err }))
  }

  try {
    return ok(decodeDID(data).did())
  } catch (err) {
    return error(new DecodeError('failed to decode bucket DID', { cause: err }))
  }
}

export const root = async (id: DID): Promise<Result<Link<unknown, number, number, Version>, EncodeFailure|InvocationFailure|DecodeError>> => {
  let input: string
  try {
    input = encodeJSON(principalFrom(id))
  } catch (err) {
    return error(new EncodeError('failed to stringify API parameters', { cause: err }))
  }

  let res: string
  try {
    res = await Root(input)
  } catch (err) {
    return error(new InvocationError('failed to invoke API', { cause: err }))
  }

  let data: Uint8Array
  try {
    data = parseJSON(res)
  } catch (err) {
    return error(new DecodeError('failed to parse API response', { cause: err }))
  }

  try {
    return ok(decodeLink(data))
  } catch (err) {
    return error(new DecodeError('failed to decode bucket root CID', { cause: err }))
  }
}

export type Object = [key: string, value: UnknownLink]

export const entries = async (id: DID, options?: { page?: number, size?: number, prefix?: string }): Promise<Result<Object[], EncodeFailure|InvocationFailure|DecodeError>> => {
  let input: string
  try {
    input = encodeJSON({ id: principalFrom(id), ...options })
  } catch (err) {
    return error(new EncodeError('failed to stringify API parameters', { cause: err }))
  }

  let res: string
  try {
    res = await Entries(input)
  } catch (err) {
    return error(new InvocationError('failed to invoke API', { cause: err }))
  }

  try {
    return ok(parseJSON<Object[]>(res))
  } catch (err) {
    return error(new DecodeError('failed to parse API response', { cause: err }))
  }
}

export const put = async (id: DID, key: string, value: UnknownLink): Promise<Result<Link<unknown, number, number, Version>, EncodeFailure|InvocationFailure|DecodeError>> => {
  let input: string
  try {
    input = encodeJSON({ id: principalFrom(id), key, value })
  } catch (err) {
    return error(new EncodeError('failed to stringify API parameters', { cause: err }))
  }

  let res: string
  try {
    res = await Put(input)
  } catch (err) {
    return error(new InvocationError('failed to invoke API', { cause: err }))
  }

  let data: Uint8Array
  try {
    data = parseJSON(res)
  } catch (err) {
    return error(new DecodeError('failed to parse API response', { cause: err }))
  }

  try {
    return ok(decodeLink(data))
  } catch (err) {
    return error(new DecodeError('failed to decode bucket root CID', { cause: err }))
  }
}

export const shareBucket = async (bucket: DID, audience: DID): Promise<Result<Delegation, EncodeFailure|InvocationFailure|DecodeError>> => {
  let input: string
  try {
    input = encodeJSON({ bucket: principalFrom(bucket), audience: principalFrom(audience) })
  } catch (err) {
    return error(new EncodeError('failed to stringify API parameters', { cause: err }))
  }

  let res: string
  try {
    res = await ShareBucket(input)
  } catch (err) {
    return error(new InvocationError('failed to invoke API', { cause: err }))
  }

  let data: Uint8Array
  try {
    data = parseJSON(res)
  } catch (err) {
    return error(new DecodeError('failed to parse API response', { cause: err }))
  }

  const extractRes = await extractDelegation(data)
  if (extractRes.error) {
    return error(new DecodeError('failed to extract delegation', { cause: extractRes.error }))
  }

  return ok(extractRes.ok)
}

export const openExternalURL = (url: string) => BrowserOpenURL(url)
