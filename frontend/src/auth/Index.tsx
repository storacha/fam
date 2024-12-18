import { NavLink, Link } from 'react-router'

export const Index = () => {
  return (
    <div>
      <NavLink
        to="/bucket/did:key:z6MkrWjRmTqtEtyvwXgQknMniPzdsCWsLoxkZXJGdbSJx1uk"
        className={({ isActive }) =>
          isActive ? "active" : ""
        }
      >
        Bucket: Test
      </NavLink>
    </div>
  )
}