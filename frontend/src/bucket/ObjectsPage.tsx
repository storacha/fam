import { MouseEventHandler, useState, useEffect, FormEvent, FormEventHandler, ChangeEventHandler } from 'react'
import { useParams, useLocation, useNavigate, NavLink, Link } from 'react-router'
import { RectangleStackIcon, PlusIcon, ShareIcon, Square2StackIcon, ArrowTopRightOnSquareIcon, TrashIcon } from '@heroicons/react/24/outline'
import { UnknownLink } from 'multiformats'
import { parse as parseLink } from 'multiformats/link'
import * as dagJSON from '@ipld/dag-json'
import { DID } from '@ucanto/interface'
import { parse as parseDID } from '@ipld/dag-ucan/did'
import { Entries } from '../../wailsjs/go/main/App'

type Object = [key: string, value: UnknownLink]

const PAGE_SIZE = 10

const getEntries = async ({ root, page, size, prefix }: { root: UnknownLink, page?: number, size?: number, prefix?: string }): Promise<Object[]> => {
  const result = await Entries(dagJSON.stringify({ root, page, size, prefix }))
  return dagJSON.parse(result)
}

export const ObjectsPage = () => {
  const params = useParams()
  const { search } = useLocation()
  const navigate = useNavigate()
  const searchParams = new URLSearchParams(search)

  const bucket = parseDID(params.did ?? '').did()
  const page = parseInt(searchParams.get('page') ?? '0')
  const size = parseInt(searchParams.get('size') ?? `${PAGE_SIZE}`)
  const prefix = searchParams.get('prefix') ?? ''
  const [objects, setObjects] = useState<Object[]>([])
  const [selections, setSelections] = useState<Record<string, Object>>({})
  const [root, setRoot] = useState(parseLink('bafybeibrqc2se2p3k4kfdwg7deigdggamlumemkiggrnqw3edrjosqhvnm'))

  useEffect(() => {
    (async () => setObjects(await getEntries({ root, page, size, prefix })))()
  }, [page, size, prefix])

  const handlePrefixChange = (prefix: string) => {
    const searchParams = new URLSearchParams({
      page: '0',
      size: size.toString(),
      prefix
    })
    console.log(`navigate to /bucket/${bucket}?${searchParams}`)
    navigate(`/bucket/${bucket}?${searchParams}`)
  }

  const handlePageChange = async (page: number) => {
    const entries = await getEntries({ root, page, size, prefix })
    if (!entries.length) return

    const searchParams = new URLSearchParams({
      page: page.toString(),
      size: size.toString(),
      prefix
    })
    console.log(`navigate to /bucket/${bucket}?${searchParams}`)
    navigate(`/bucket/${bucket}?${searchParams}`)
  }

  return (
    <div className='h-screen flex'>
      <div className='flex-none w-14 border-r border-dashed border-hot-red bg-clip-padding text-center pt-3 pb-1'>
        <NavLink to={`/bucket/${bucket}`} style={{ lineHeight: 0 }} className={({ isActive }) => `${isActive ? 'bg-hot-red text-white p-1 m-1 rounded-full' : 'text-hot-red hover:text-black p-2'} inline-block`} title='Items'>
          <RectangleStackIcon className='inline-block size-6' />
        </NavLink>
        <NavLink to={`/bucket/${bucket}/put`} style={{ lineHeight: 0 }} className={({ isActive }) => `${isActive ? 'bg-hot-red text-white p-1 m-1 rounded-full' : 'text-hot-red hover:text-black p-2'} inline-block`} title='Put Object'>
          <PlusIcon className='inline-block size-6' />
        </NavLink>
        <button type='button' className={`p-2 text-hot-red ${Object.keys(selections).length ? 'hover:text-black' : 'opacity-25'}`} disabled={!Object.keys(selections).length} title={`Delete ${Object.keys(selections).length > 1 ? 'Objects' : Object.keys(selections)[0]}`}>
          <TrashIcon className='inline-block size-6' />
        </button>
        <NavLink to={`/bucket/${bucket}/share`} style={{ lineHeight: 0 }} className={({ isActive }) => `${isActive ? 'bg-hot-red text-white p-1 m-1 rounded-full' : 'text-hot-red hover:text-black p-2'} inline-block`} title='Share Bucket'>
          <ShareIcon className='inline-block size-6' />
        </NavLink>
      </div>
      <div className='flex-auto p-3 overflow-scroll'>
        <div className='flex flex-col h-full'>
          <div className='flex-none'>
            <SearchForm prefix={prefix} onPrefixChange={handlePrefixChange} />
          </div>
          <div className='flex-grow'>
            {page === 0 && prefix === '' && !objects.length ? (
              <div className='flex flex-col justify-center h-full'>
                <p className='font-epilogue text-center mb-2'>No Objects</p>
                <p className='font-epilogue text-hot-red hover:text-black text-sm text-center cursor-pointer'>
                  <NavLink to={`/bucket/${bucket}/put`} title='Put Object' className='inline-block mr-1 align-text-bottom'>
                    <PlusIcon className='size-4' />
                  </NavLink>
                  Put an Object
                </p>
              </div>
            ) : (
              <>
                <ObjectList objects={objects} selections={selections} onSelectionsChange={setSelections} />
                <Pagination page={page} size={size} objectCount={objects.length} onPageChange={handlePageChange} />
              </>
            )}
          </div>
          <div className='flex-none py-4'>
            <div className='font-mono text-xs text-center text-gray-400'>
              {params.did}
              <button type='button' className='hover:text-black px-1' title='Copy Bucket DID'>
                <Square2StackIcon className='inline-block size-4' />
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}

const SearchForm = ({ prefix, onPrefixChange }: { prefix: string, onPrefixChange: (prefix: string) => void }) => {
  const [search, setSearch] = useState(prefix)
  const handleSearchSubmit: FormEventHandler = e => {
    e.preventDefault()
    onPrefixChange(search)
  }
  return (
    <form onSubmit={handleSearchSubmit} className='max-w-md mx-auto mb-6'>
      <label htmlFor='default-search' className='mb-2 text-sm font-medium sr-only'>Search</label>
      <div className='relative'>
        <div className='absolute inset-y-0 start-0 flex items-center ps-4 pointer-events-none'>
          <svg className='w-4 h-4 text-hot-red' aria-hidden='true' xmlns='http://www.w3.org/2000/svg' fill='none' viewBox='0 0 20 20'>
            <path stroke='currentColor' strokeLinecap='round' strokeLinejoin='round' strokeWidth='2' d='m19 19-4-4m0-7A7 7 0 1 1 1 8a7 7 0 0 1 14 0Z' />
          </svg>
        </div>
        <input type='search' id='default-search' className='font-epilogue block w-full p-3 ps-10 text-sm border border-hot-red rounded-full bg-gray-50 focus:ring-red-500 focus:border-red-500 ' placeholder='Search by key prefix' onChange={e => setSearch(e.target.value)} />
        <button type='submit' className='font-epilogue text-white absolute end-1.5 bottom-1.5 bg-hot-red hover:bg-red-800 focus:ring-4 focus:outline-none focus:ring-red-300 font-medium rounded-full text-sm px-4 py-2'>Search</button>
      </div>
    </form>
  )
}

interface ObjectListProps {
  objects: Object[]
  selections: Record<string, Object>
  onSelectionsChange: (selections: Record<string, Object>) => void
}

const ObjectList = ({ objects, selections, onSelectionsChange }: ObjectListProps) => {
  const allSelected = () => objects.length > 0 && Object.keys(selections).length === objects.length
  const handleSelectAllChange = () => {
    if (allSelected()) {
      onSelectionsChange({})
    } else {
      onSelectionsChange(Object.fromEntries(objects.map(o => [o[0], o])))
    }
  }
  const handleSelectChange: ChangeEventHandler<HTMLInputElement> = e => {
    const object = objects.find(o => o[0] === e.currentTarget.value)
    if (!object) return
    if (selections[object[0]]) {
      onSelectionsChange(Object.fromEntries(Object.entries(selections).filter(([k]) => k !== object[0])))
    } else {
      onSelectionsChange({ ...selections, [object[0]]: object })
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
            <th scope='col' className='px-6 py-4 w-full'>Key</th>
            <th scope='col' className='px-6 py-4'>Value</th>
          </tr>
        </thead>
        <tbody>
          {objects.map(([key, value]) => {
            return (
              <tr key={key} className='border-b border-hot-red-light last:border-0 hover:bg-hot-yellow-light transition-colors'>
                <td className='w-4 p-4'>
                  <div className='flex items-center'>
                    <input id='checkbox-table-search-1' type='checkbox' value={key} className='w-4 h-4 text-red-600 bg-gray-100 border-gray-300 rounded focus:ring-red-500' checked={!!selections[key]} onChange={handleSelectChange} />
                    <label htmlFor='checkbox-table-search-1' className='sr-only'>checkbox</label>
                  </div>
                </td>
                <th scope='row' className='px-6 py-4'>
                  <div className='max-w-lg font-medium text-xs text-gray-900 whitespace-nowrap overflow-hidden text-ellipsis' title={key}>
                    {key}
                  </div>
                </th>
                <td className='px-6 py-4'>
                  <Link to={`https://w3s.link/ipfs/${value}`} className='block font-mono text-xs whitespace-nowrap hover:text-hot-red'>
                    {value.toString()}
                    <ArrowTopRightOnSquareIcon className='inline-block size-4 ml-1 align-text-bottom' />
                  </Link>
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

const Pagination = ({ page, size, objectCount, onPageChange }: { page: number, size: number, objectCount: number, onPageChange: (page: number) => void }) => {
  const onPrevClick: MouseEventHandler = e => {
    e.preventDefault()
    if (page !== 0) onPageChange(page - 1)
  }
  const onNextClick: MouseEventHandler = e => {
    e.preventDefault()
    if (objectCount === size) onPageChange(page + 1)
  }
  return (
    <nav className='flex items-center flex-column flex-wrap md:flex-row justify-between p-4' aria-label='Table navigation'>
      <span className='text-sm font-normal text-gray-500 dark:text-gray-400 mb-4 md:mb-0 block w-full md:inline md:w-auto'>Showing <span className='font-semibold text-gray-900'>{(page*size+1).toLocaleString()}-{(page*size+objectCount).toLocaleString()}</span></span>
      <ul className='inline-flex -space-x-px rtl:space-x-reverse text-sm h-8'>
        <li>
          <a href='#' className='flex items-center justify-center px-3 h-8 ms-0 leading-tight text-gray-500 bg-white border border-gray-300 rounded-s-lg hover:bg-gray-100 hover:text-gray-700' onClick={onPrevClick}>Previous</a>
        </li>
        <li>
          <a href='#' className='flex items-center justify-center px-3 h-8 leading-tight text-gray-500 bg-white border border-gray-300 rounded-e-lg hover:bg-gray-100 hover:text-gray-700' onClick={onNextClick}>Next</a>
        </li>
      </ul>
    </nav>
  )
}
