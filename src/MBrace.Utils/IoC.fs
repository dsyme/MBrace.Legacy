namespace Nessos.MBrace.Utils

    open Nessos.Thespian.ConcurrencyTools

    type DependencyContainer (id : string) =

        // map : type qualified name * optional parameter -> (unit -> obj) factory
        let container = Atom.atom Map.empty<string * string option, unit -> obj>

        static let lift memoize (f : unit -> 'T) =
            if memoize then
                let v = lazy (f () :> obj)
                fun () -> v.Value
            else
                fun () -> f () :> obj

        static member private GetKey<'T> (param : string option) = 
            typeof<'T>.AssemblyQualifiedName, param

        member __.ContainerId = id

        member __.Register(factory : unit -> 'T, ?parameter : string, ?memoize:bool, ?overwrite:bool) =
            let overwrite = defaultArg overwrite false
            let memoize = defaultArg memoize true

            let key = DependencyContainer.GetKey<'T> parameter

            let success =
                container.Transact(fun instance -> 
                    if not overwrite && instance.ContainsKey key then
                        instance, false
                    else
                        instance.Add(key, lift memoize factory), true)

            if success then ()
            else
                match parameter with
                | None -> failwithf "IoC: dependency of type '%s' has already been registered." typeof<'T>.Name
                | Some p -> failwithf "IoC: dependency of type '%s' and parameter '%s' has already been registered." typeof<'T>.Name p

        member __.IsRegistered<'T> ?parameter = container.Value.ContainsKey <| DependencyContainer.GetKey<'T> parameter
                
        member __.TryResolve<'T> ?parameter =
            match container.Value.TryFind <| DependencyContainer.GetKey<'T> parameter with
            | None -> None
            | Some f -> Some (f () :?> 'T)

        member __.Resolve<'T> ?parameter =
            match __.TryResolve<'T> (?parameter = parameter) with
            | Some t -> t
            | None ->
                match parameter with
                | None -> failwithf "IoC: no dependency of type '%s' has been registered." typeof<'T>.Name
                | Some p -> failwithf "IoC: no depoendency of type '%s' and parameter '%s' has been registered." typeof<'T>.Name p


    type IoC private () =

        static let container = new DependencyContainer("Default IoC Container")

        static member TryResolve<'T> ?parameter = container.TryResolve<'T>(?parameter = parameter)

        static member Resolve<'T> ?parameter = container.Resolve<'T>(?parameter = parameter)

        static member IsRegistered<'T> ?parameter = container.IsRegistered<'T>(?parameter = parameter)

        static member Register<'T>(factory : unit -> 'T, ?parameter : string, ?overwrite : bool, ?memoize : bool) =
            container.Register<'T>(factory, ?parameter = parameter, ?overwrite = overwrite, ?memoize = memoize)

        static member RegisterValue<'T>(value : 'T, ?parameter : string, ?overwrite : bool) =
            container.Register<'T>((fun () -> value), ?parameter = parameter, ?overwrite = overwrite, memoize = false)