import { ChangeEventHandler, useState } from 'react'
import { CAR } from '@ucanto/core'
import { parse as parseDID } from '@ipld/dag-ucan/did'
import { NavLink, useParams } from 'react-router'
import { PlusIcon, RectangleStackIcon, ShareIcon, Square2StackIcon, TrashIcon } from '@heroicons/react/24/outline'
import { base64 } from 'multiformats/bases/base64'
import { identity } from 'multiformats/hashes/identity'
import { create as createLink } from 'multiformats/link'
import * as API from '../api'

export const Share = () => {
  const params = useParams()
  const bucket = parseDID(params.did ?? '').did()
  const [audience, setAudience] = useState('')
  const [delegation, setDelegation] = useState('')

  const handleAudienceChange: ChangeEventHandler<HTMLInputElement> = async e => {
    setAudience(e.currentTarget.value)
    let audience
    try {
      audience = parseDID(e.currentTarget.value).did()
    } catch (err) {
      console.warn('parsing audience', err)
      return
    }

    const shareRes = await API.shareBucket(bucket, audience)
    if (shareRes.error) return console.error(shareRes.error) // TODO handle error

    const archiveRes = await shareRes.ok.archive()
    if (archiveRes.error) return console.error(archiveRes.error) // TODO handle error

    const link = createLink(CAR.code, identity.digest(archiveRes.ok))
    setDelegation(link.toString(base64))
  }

  return (
    <div className='h-screen flex'>
      <div className='flex-none w-14 border-r border-dashed border-hot-red bg-clip-padding text-center pt-3 pb-1'>
        <NavLink to={`/bucket/${bucket}`} style={{ lineHeight: 0 }} className={() => `${location.pathname === `/bucket/${bucket}` ? 'bg-hot-red text-white p-1 m-1 rounded-full' : 'text-hot-red hover:text-black p-2'} inline-block`} title='Items'>
          <RectangleStackIcon className='inline-block size-6' />
        </NavLink>
        <NavLink to={`/bucket/${bucket}/put`} style={{ lineHeight: 0 }} className={({ isActive }) => `${isActive ? 'bg-hot-red text-white p-1 m-1 rounded-full' : 'text-hot-red hover:text-black p-2'} inline-block`} title='Put Object'>
          <PlusIcon className='inline-block size-6' />
        </NavLink>
        <button type='button' className='p-2 text-hot-red opacity-25' disabled={true}>
          <TrashIcon className='inline-block size-6' />
        </button>
        <NavLink to={`/bucket/${bucket}/share`} style={{ lineHeight: 0 }} className={({ isActive }) => `${isActive ? 'bg-hot-red text-white p-1 m-1 rounded-full' : 'text-hot-red hover:text-black p-2'} inline-block`} title='Share Bucket'>
          <ShareIcon className='inline-block size-6' />
        </NavLink>
      </div>
      <div className='flex-auto p-3 overflow-scroll'>
        <div className='flex flex-col justify-center items-center h-full px-6 lg:px-24'>
          <p className='font-epilogue text-center mb-2'>Share with:</p>
          <input type='text' onChange={handleAudienceChange} value={audience} className='font-mono p-2 border rounded-lg' style={{ width: '36rem' }} required placeholder='did:key:...' />
          <p className='font-epilogue text-xs mt-2 mb-10'>(who should be granted access)</p>
          <p className='font-epilogue text-center mb-2'>Send the bucket delegation below:</p>
          <textarea className='font-mono p-2 border rounded-xl mb-3 w-full h-96 sm:h-80 md:h-60 lg:h-52' value={delegation} readOnly></textarea>
          <button type='button' className='font-epilogue text-hot-red hover:text-black text-sm text-center cursor-pointer'>
            <Square2StackIcon  className='size-5 inline-block mr-1 align-text-bottom' />
            Copy
          </button>
        </div>
      </div>
    </div>
  )
}
