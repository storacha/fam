import { useState, useEffect, ChangeEventHandler } from 'react'
import { Delegation, DID } from '@ucanto/interface'
import { NavLink } from 'react-router'
import { ArrowDownOnSquareIcon } from '@heroicons/react/24/outline'
import { Link } from 'multiformats'
import * as API from '../api'

export const Index = () => {
  const [buckets, setBuckets] = useState(new Map<DID, Delegation>())
  const [selections, setSelections] = useState(new Set<DID>())
  const [roots, setRoots] = useState(new Map<DID, Link>)

  useEffect(() => {
    (async () => {
      const buckets = await API.buckets()
      if (buckets.error) return console.error(buckets.error) // TODO handle error
      setBuckets(buckets.ok)
    })()
  })

  useEffect(() => {
    (async () => {
      if (!buckets.size) return
      const roots = await Promise.all([...buckets.keys()].map(async id => {
        const root = await API.root(id)
        if (root.error) throw root.error // TODO handle error
        return [id, root.ok] as [DID, Link]
      }))
      setRoots(new Map(roots))
    })()
  }, [buckets])

  if (!buckets.size) {
    return (
      <div className='flex flex-col justify-center h-full'>
        <p className='font-epilogue text-center mb-2'>No Buckets</p>
        <p className='font-epilogue text-hot-red hover:text-black text-sm text-center cursor-pointer'>
          <NavLink to='/bucket/import' title='Import Bucket'>
            <ArrowDownOnSquareIcon className='size-5 inline-block mr-1 align-text-bottom' /> Import a Bucket
          </NavLink>
        </p>
      </div>
    )
  }

  const entries = [...buckets.keys()].sort((a, b) => a[0] > b[0] ? -1 : 1)
  return (
    <div className='p-3 overflow-scroll'>
      <BucketList buckets={entries} roots={roots} selections={selections} onSelectionsChange={setSelections} />
    </div>
  )
}

interface BucketListProps {
  buckets: DID[]
  roots: Map<DID, Link>
  selections: Set<DID>
  onSelectionsChange: (selections: Set<DID>) => void
}

const BucketList = ({ buckets, roots, selections, onSelectionsChange }: BucketListProps) => {
  const allSelected = () => buckets.length > 0 && selections.size === buckets.length
  const handleSelectAllChange = () => {
    if (allSelected()) {
      onSelectionsChange(new Set())
    } else {
      onSelectionsChange(new Set(buckets))
    }
  }
  const handleSelectChange: ChangeEventHandler<HTMLInputElement> = e => {
    const selected = buckets.find(id => id === e.currentTarget.value)
    if (!selected) return
    if (selections.has(selected)) {
      onSelectionsChange(new Set([...selections].filter(s => s !== selected)))
    } else {
      onSelectionsChange(new Set([...selections, selected]))
    }
  }

  return (
    <div className='relative overflow-x-auto'>
      <table className='w-full text-sm text-left rtl:text-right text-gray-500'>
        <thead className='text-xs text-gray-700 uppercase'>
          <tr>
            <th scope='col' className='p-4'>
              <div className='flex items-center'>
                <input id='checkbox-all-search' type='checkbox' className='w-4 h-4 text-red-600 bg-gray-100 border-gray-300 rounded focus:ring-red-500' onChange={handleSelectAllChange} checked={allSelected()} />
                <label htmlFor='checkbox-all-search' className='sr-only'>checkbox</label>
              </div>
            </th>
            <th scope='col' className='px-6 py-4 w-full'>Bucket</th>
          </tr>
        </thead>
        <tbody>
          {buckets.map(id => {
            return (
              <tr key={id} className='border-b border-hot-red-light last:border-0 hover:bg-hot-yellow-light transition-colors'>
                <td className='w-4 p-4'>
                  <div className='flex items-center'>
                    <input id='checkbox-table-search-1' type='checkbox' value={id} className='w-4 h-4 text-red-600 bg-gray-100 border-gray-300 rounded focus:ring-red-500' checked={selections.has(id)} onChange={handleSelectChange} />
                    <label htmlFor='checkbox-table-search-1' className='sr-only'>checkbox</label>
                  </div>
                </td>
                <th scope='row'>
                  <NavLink to={`/bucket/${id}`} className='max-w-lg font-medium font-mono text-xs text-gray-900 group hover:text-hot-red block px-6 py-4' title={id}>
                    {id}<br/>
                    <span className='text-gray-500 group-hover:text-hot-red'>{roots.get(id)?.toString() ?? 'Unknown'}</span>
                  </NavLink>
                </th>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}
