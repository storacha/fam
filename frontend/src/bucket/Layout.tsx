import { Outlet, NavLink, useLocation } from 'react-router'
import { Cog6ToothIcon, HomeIcon, ArrowDownOnSquareIcon } from '@heroicons/react/24/outline'

export const Layout = () => {
  const location = useLocation()
  console.log(location.pathname)
  return (
    <div className='h-screen flex'>
      <div className='flex-none w-14 border-r border-dashed border-hot-red bg-clip-padding bg-storacha-sideways'>
        <div className='h-screen flex flex-col justify-between text-center pt-3 pb-1'>
          <div>
            <NavLink to="/bucket" style={{ lineHeight: 0 }} className={() => `${location.pathname === '/' ? 'bg-hot-red text-white m-1 p-1 rounded-full' : 'text-hot-red hover:text-black p-2'} inline-block`} title='Buckets'>
              <HomeIcon className="inline-block size-6" />
            </NavLink>
            <NavLink to="/bucket/import" style={{ lineHeight: 0 }} className={({ isActive }) => `${isActive ? 'bg-hot-red text-white m-1 p-1 rounded-full' : 'text-hot-red hover:text-black p-2'} inline-block`} title='Import Bucket'>
              <ArrowDownOnSquareIcon className="inline-block size-6" />
            </NavLink>
          </div>
          <div>
            <NavLink to="/settings" style={{ lineHeight: 0 }} className={({ isActive }) => `${isActive ? 'bg-hot-red text-white m-1 p-1 rounded-full' : 'text-hot-red hover:text-black p-2'} inline-block`} title='Settings'>
              <Cog6ToothIcon className="inline-block size-6" />
            </NavLink>
          </div>
        </div>
      </div>
      <div className='flex-auto overflow-scroll'>
        <Outlet />
      </div>
    </div>
  )
}
