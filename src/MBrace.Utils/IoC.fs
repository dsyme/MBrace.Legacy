namespace Nessos.MBrace.Utils

    open Nessos.Thespian.ConcurrencyTools

    type RegistrationMode = Singleton | Factory
    type OverrideBehaviour = 
            | Override
            | DoNothing
            | Fail

    type private DependencyContainer<'T> () =
        static let container : Atom<Map<string option, unit -> 'T>> = Atom.atom Map.empty

        static let t = typeof<'T>

        static let morph mode (f : unit -> 'T) =
            match mode with
            | Singleton ->
                let singleton = lazy(f ())
                fun () -> singleton.Value
            | Factory -> f

        static member Register (mode, behaviour, param, factory : unit -> 'T) =
            let failure =
                container.Transact(fun instance ->
                    match instance.ContainsKey param, behaviour with
                    | true, Fail -> instance, true
                    | true, DoNothing -> instance, false
                    | _, _ -> instance.Add(param, morph mode factory), false
                )
            
            if failure then
                match param with
                | None -> failwithf "IoC : type %s has already been registered" t.Name
                | Some param -> failwithf "IoC : type %s with parameter \"%s\" has already been registered" t.Name param

        static member IsRegistered param = container.Value.ContainsKey param

        static member TryResolve param = 
            match container.Value.TryFind param with
            | None -> None
            | Some f ->
                try Some (f ())
                with e -> 
                    match param with
                    | None -> failwithf "IoC : factory method for type %s has thrown an exception:\n %s" t.Name <| e.ToString()
                    | Some param -> 
                        failwithf "IoC : factory method for type %s with parameter \"%s\" has thrown an exception:\n %s" t.Name param <| e.ToString()

        static member Resolve param =
            match DependencyContainer<'T>.TryResolve param with
            | Some v -> v
            | None ->
                match param with
                | None -> failwithf "IoC : no instace of type %s has been registered" t.Name
                | Some param -> failwithf "IoC : no instance of type %s with parameter \"%s\" has been registered" t.Name param


    // we use the dependency container mechanism itself to store IoC settings
    // TODO : remove
    type private IoCSettings = 
        { 
            Mode : RegistrationMode 
            Behaviour : OverrideBehaviour 
        }
            

    type IoC private () =

        static let setConfiguration (conf : IoCSettings) =
            DependencyContainer<_>.Register (Factory, Override, None, fun () -> conf)
        static let getConfiguration () =
            DependencyContainer<IoCSettings>.Resolve None

        // set defaults
        static do setConfiguration { Mode = Singleton ; Behaviour = Fail }

        static member TryResolve<'T> ?param = DependencyContainer<'T>.TryResolve param

        static member Resolve<'T> ?param = DependencyContainer<'T>.Resolve param

        static member IsRegistered<'T> ?param = DependencyContainer<'T>.IsRegistered param

        static member SetOverrideBehaviour (behaviour : OverrideBehaviour) =
            let conf = getConfiguration ()
            if conf.Behaviour <> behaviour then
                setConfiguration { conf with Behaviour = behaviour }

        static member SetRegistrationMode (mode : RegistrationMode) =
            let conf = getConfiguration ()
            if conf.Mode <> mode then
                setConfiguration { conf with Mode = mode }

        static member Register<'T> (factory : unit -> 'T, ?parameter : string, ?mode : RegistrationMode, ?behaviour : OverrideBehaviour) =
            let conf = getConfiguration ()
            let mode = defaultArg mode conf.Mode
            let behaviour = defaultArg behaviour conf.Behaviour
            DependencyContainer<'T>.Register (mode, behaviour, parameter, factory)

        static member RegisterValue<'T> (value : 'T, ?parameter : string, ?behaviour : OverrideBehaviour) =
            let behaviour = match behaviour with None -> getConfiguration().Behaviour | Some b -> b
            DependencyContainer<'T>.Register (Factory, behaviour, parameter, fun () -> value)

        static member UpdateValue<'T> (value : 'T, ?parameter : string) =
            DependencyContainer<'T>.Register (Factory, Override, parameter, fun () -> value)