(*** hide ***)
// This block of code is omitted in the generated HTML documentation. Use 
// it to define helpers that you do not want to show in the documentation.
#I "../../bin"
#I "../../src/MBrace.Client/" // preamble.fsx

(**

# MBrace : An open source framework for large-scale distributed computation and data processing written in F#.

<div class="row">
  <div class="span1"></div>
  <div class="span6">
    <div class="well well-small" id="nuget">
      The MBrace framework can be <a href="https://nuget.org/packages/MBrace">installed from NuGet</a>:
      <pre>PM> Install-Package MBrace.Runtime
PM> Install-Package MBrace.Core</pre>
    </div>
  </div>
  <div class="span1"></div>
</div>

## Example

This example demonstrates a basic cloud computation.

*)
#load "preamble.fsx"

open Nessos.MBrace
open Nessos.MBrace.Client

let runtime = MBrace.InitLocal 3

runtime.Run <@ cloud { return 42 } @>

(**
## Documentation & Technical Overview

Coming soon.
 
## Contributing and copyright

The project is hosted on [GitHub][gh] where you can [report issues][issues], fork 
the project and submit pull requests.

The library is available under the MIT License. 
For more information see the [License file][license] in the GitHub repository. 

  [gh]: https://github.com/nessos/MBrace
  [issues]: https://github.com/nessos/MBrace/issues
  [license]: https://github.com/nessos/MBrace/blob/master/License.md
*)
