namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("MBrace")>]
[<assembly: AssemblyProductAttribute("MBrace")>]
[<assembly: AssemblyCompanyAttribute("Nessos Information Technologies")>]
[<assembly: AssemblyCopyrightAttribute("© Nessos Information Technologies.")>]
[<assembly: AssemblyTrademarkAttribute("{m}brace")>]
[<assembly: AssemblyVersionAttribute("0.5.8")>]
[<assembly: AssemblyFileVersionAttribute("0.5.8")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "0.5.8"
