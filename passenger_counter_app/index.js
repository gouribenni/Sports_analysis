

let num1=8
let num2=2 

document.getElementById("num1-el").textContent=num1 
document.getElementById("num1-el").textContent=num2 

function add(){
    document.getElementById("sum-el").innerText= num1+num2;
}

function subtract(){
    document.getElementById("sum-el").innerText= num1-num2;
}

function multiply(){
    document.getElementById("sum-el").innerText= num1*num2;
}


function divide(){
    document.getElementById("sum-el").innerText= num1/num2;
}
gsap.registerPlugin(MotionPathPlugin);
gsap.registerPlugin(CustomEase);
//gsap.from("#Rectangle",{duration:5,y:300,backgroundColor:"#560563",borderRadius:"10%",border:"2px solid white",ease:"bounce"});
//gsap.to("#Star",{duration:5,y:300,backgroundColor:"#560563",borderRadius:"10%",border:"2px solid white",ease:"bounce"});
CustomEase.create("hop", ".17,.67,.83,.00");
gsap.set(".Star", {scale: 0.5, autoAlpha: 1});
gsap.set(".Rectangle_move", {scale: 0.5, autoAlpha: 1});

gsap.from("#Star", {
    duration: 1, 
    repeat: 3,
    //repeatDelay: 3,
    yoyo: true,
    ease: "power1.inOut",
    opacity:1,
    motionPath:{
      path: "#Path-2",
      align: "#Path-2",
      autoRotate: false,
      alignOrigin: [0.5, 0.5]
    }
  });



gsap.to("#Rectangle_move", {
    duration: 1, 
    repeat: 3,
    //repeatDelay: 3,
    //yoyo: true,
    immediateRender:false,
    //ease: "hop",
    scale:0.6,
    stagger: 1,
    motionPath:{
      path: "#Path-3",
      align: "#Path-3",
      autoRotate: false,
      alignOrigin: [0.5, 0.5]
    }
  });

let t1
  //gsap.timeline({delay: 2.5})
// gsap.set(".station, .logo",{transformOrigin:"50% 50%"})
//gsap.to(".logo, .station",{duration:5,x:100,y:100,backgroundColor:"#560563",borderRadius:"10%",border:"2px solid white",rotation:"360"});

//welcomeEl = document.getElementById("welcome-el").innerText
//nameGreeting = "Hi I am Nitin"
//console.log(nameGreeting)
//document.getElementById("welcome-el").innerText=nameGreeting

