import { FormEventHandler, useEffect, useState } from 'react'
import { CAR } from '@ucanto/core'
import { delegate } from '@ucanto/core/delegation'
import { ed25519 } from '@ucanto/principal'
import { parse as parseProof } from '@storacha/client/proof'
import { base64 } from 'multiformats/bases/base64'
import { identity } from 'multiformats/hashes/identity'
import { create as createLink } from 'multiformats/link'
import { useNavigate } from 'react-router'
import { ArrowDownOnSquareIcon, Square2StackIcon } from '@heroicons/react/24/outline'
import * as API from '../api'
import { DID } from '@ucanto/interface'

export const Import = () => {
  const [agentID, setAgentID] = useState<DID|undefined>()
  const [proof, setProof] = useState('')
  const navigate = useNavigate()

  useEffect(() => {
    (async () => {
      if (agentID) return
      const signer = await API.id()
      if (signer.error) return console.error(signer.error) // TODO handle error
      setAgentID(signer.ok.did())
    })()
  }, [agentID])

  useEffect(() => {
    (async () => {
      if (proof) return
      const signer = await API.id()
      if (signer.error) return console.error(signer.error) // TODO handle error
      const resource = (await ed25519.generate()).did()
      const delegation = await delegate({
        issuer: signer.ok,
        audience: signer.ok,
        capabilities: [
          { can: 'clock/*', with: resource, nb: {} },
          { can: 'space/blob/*', with: resource, nb: {} }
        ]
      })
      const res = await delegation.archive()
      if (res.error) return console.error(res.error) // TODO handle error

      const link = createLink(CAR.code, identity.digest(res.ok))
      setProof(link.toString(base64))
    })()
  }, [proof])

  const handleSubmit: FormEventHandler<HTMLFormElement> = async e => {
    e.preventDefault()
    if (!proof) return
    try {
      const id = await API.addBucket(await parseProof(proof))
      if (id.error) throw id.error
      navigate(`/bucket/${id.ok}`)
    } catch (err) {
      // TODO: handle error
      return console.error(err)
    }
  }

  return (
    <form className='flex flex-col justify-center items-center h-full px-6 lg:px-24' onSubmit={handleSubmit}>
      <p className='font-epilogue text-center mb-2'>Your agent DID:</p>
      <p className='font-mono text-center rounded-full px-4 py-2 bg-hot-yellow-light'>{agentID}</p>
      <p className='font-epilogue text-xs my-2'>(share this with someone who can grant access)</p>
      <button type='button' className='font-epilogue text-hot-red hover:text-black text-sm text-center cursor-pointer mb-10'>
        <Square2StackIcon  className='size-5 inline-block mr-1 align-text-bottom' />
        Copy
      </button>
      <p className='font-epilogue text-center mb-2'>Paste your bucket delegation below:</p>
      <textarea value={proof} onChange={e => setProof(e.currentTarget.value)} className='font-mono p-2 border rounded-xl mb-3 w-full h-96 sm:h-80 md:h-60 lg:h-52'></textarea>
      <button type='submit' className='font-epilogue text-hot-red hover:text-black text-sm text-center cursor-pointer'>
        <ArrowDownOnSquareIcon className='size-5 inline-block mr-1 align-text-bottom' />
        Import Bucket
      </button>
    </form>
  )
}