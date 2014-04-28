namespace Nessos.MBrace.Core

    [<AutoOpen>]
    module CompilerStateMonad = 
        
        type CompilerState<'T, 'S> = S of ('S -> ('T * 'S))

        let (>>=) ms f =
           S (fun s ->
                    let (S statef) = ms 
                    let (a, s') = statef s
                    let (S statef') = f a in statef' s')
 
        let unit a = S (fun s -> (a, s))
 
        type CompilerStateBuilder() =
          member self.Bind(ms, f) = ms >>= f
          member self.Return a = unit a
          member self.ReturnFrom ms = ms
          member self.Zero() = S (fun s -> ((), s))
          member self.Combine (first, second) = first >>= (fun _ -> second)
          member self.Delay f = f ()
            
 
        let state = new CompilerStateBuilder()
 
        let getState = S (fun s -> (s, s))
        let setState s = S (fun _ -> ((), s)) 

        let rec sequence list = 
            match list with
            | [] -> unit []
            | ms :: mss -> ms >>= (fun v -> sequence mss >>= (fun vs -> unit (v :: vs)))

        let execute m s = let (S f) = m in f s 
