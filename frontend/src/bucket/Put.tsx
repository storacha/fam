import { FormEventHandler, useState } from 'react'
import { parse as parseDID } from '@ipld/dag-ucan/did'
import { NavLink, useLocation, useNavigate, useParams } from 'react-router'
import { PlusIcon, RectangleStackIcon, ShareIcon, TrashIcon } from '@heroicons/react/24/outline'
import { parse as parseLink } from 'multiformats/link'
import * as API from '../api'

export const Put = () => {
  const params = useParams()
  const bucket = parseDID(params.did ?? '').did()
  const [key, setKey] = useState('')
  const [value, setValue] = useState('')
  const location = useLocation()
  const navigate = useNavigate()

  const handleSubmit: FormEventHandler<HTMLFormElement> = async e => {
    e.preventDefault()
    if (!key || !value) return
    const res = await API.put(bucket, key, parseLink(value))
    if (res.error) throw res.error // TODO handle error
    navigate(`/bucket/${bucket}`)
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
        <form className='flex flex-col justify-center items-center h-full px-6 lg:px-24' onSubmit={handleSubmit} autoComplete='off'>
          <p className='font-epilogue text-center mb-2'>
            <span className='inline-block text-right mr-3 w-14'>Key:</span>
            <input type='text' onChange={e => setKey(e.currentTarget.value)} value={key} className='font-mono text-xs p-2 border rounded-lg' style={{ width: '30rem' }} required autoComplete='new-text' />
          </p>
          <p className='font-epilogue text-center mb-3'>
            <span className='inline-block text-right mr-3 w-14'>Value:</span>
            <input type='text' onChange={e => setValue(e.currentTarget.value)} value={value} className='font-mono text-xs p-2 border rounded-lg' style={{ width: '30rem' }} required  autoComplete='new-text' placeholder='bafy...' />
          </p>
          <button type='submit' className='font-epilogue text-hot-red hover:text-black text-sm text-center cursor-pointer'>
            <PlusIcon className='size-5 inline-block mr-1 align-text-bottom' />
            Put Object
          </button>
        </form>
      </div>
    </div>
  )
}