import {useState} from 'react';
import logo from '../assets/images/logo-universal.png';
import {Greet} from "../../wailsjs/go/main/App";

export const Index = () => {
  const [resultText, setResultText] = useState("Please enter your name below 👇");
  const [name, setName] = useState('');
  const updateName = (e: any) => setName(e.target.value);
  const updateResultText = (result: string) => setResultText(result);

  function greet() {
    Greet(name).then(updateResultText);
  }

  return (
    <div id="App">
      <img src={logo} id="logo" alt="logo"/>
      <div id="result" className="text-3xl font-bold underline">{resultText}</div>
      <div id="input" className="input-box">
        <input id="name" className="input" onChange={updateName} autoComplete="off" name="input" type="text"/>
        <button className="btn" onChange={greet}>Greet</button>
      </div>
    </div>
  )
}
