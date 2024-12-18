import React from 'react'
import { createRoot } from 'react-dom/client'
import { BrowserRouter, Routes, Route } from 'react-router'
import './style.css'
import { Layout as AuthLayout } from './auth/Layout'
import { Index as AuthIndex } from './auth/Index'
import { Layout as BucketLayout } from './bucket/Layout'
import { Index as BucketIndex } from './bucket/Index'
import { ObjectsPage as BucketObjectsPage } from './bucket/ObjectsPage'

const container = document.getElementById('root')
const root = createRoot(container!)

root.render(
  <React.StrictMode>
    <BrowserRouter>
      <Routes>
        <Route path='/' element={<AuthLayout />}>
          <Route index element={<AuthIndex />} />
        </Route>
        <Route path='/bucket' element={<BucketLayout />}>
          <Route index element={<BucketIndex />} />
          <Route path=":did" element={<BucketObjectsPage />} />
        </Route>
      </Routes>
    </BrowserRouter>
  </React.StrictMode>
)
