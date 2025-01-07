import { useEffect } from 'react'
import { useNavigate } from 'react-router'

export const Index = () => {
  const navigate = useNavigate()
  useEffect(() => { navigate('/bucket') })
  return null
}