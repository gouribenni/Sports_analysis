
let lapsCompleted = 0
function increment(){
    lapsCompleted+=1
    document.getElementById("count-el").innerText=lapsCompleted
}

let saveEl = document.getElementById("save-el")

console.log(saveEl)
let string_saved_entries = ""
function save(){
    string_saved_entries= lapsCompleted.toString()+" -"
    console.log(string_saved_entries)
    saveEl.innerText=saveEl.innerText+string_saved_entries
    lapsCompleted=0
    document.getElementById("count-el").innerText=lapsCompleted
}




//welcomeEl = document.getElementById("welcome-el").innerText
//nameGreeting = "Hi I am Nitin"
//console.log(nameGreeting)
//document.getElementById("welcome-el").innerText=nameGreeting