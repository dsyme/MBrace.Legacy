(*** hide ***)
// This block of code is omitted in the generated HTML documentation. Use 
// it to define helpers that you do not want to show in the documentation.
#I "../../bin/"
#I "../../src/MBrace.Client/"

#load "bootstrap.fsx"

#r "FsPickler.dll"

open Nessos.MBrace
open Nessos.MBrace.Lib
open Nessos.MBrace.Client

open Nessos.FsPickler

(**

# MBrace Client API

The following article provides an overview of the MBrace client API,
the collection of types and methods used for interacting with the MBrace runtime.

## Installation

These can be accessed by adding the [`MBrace.Client`](http://www.nuget.org/packages/MBrace.Client) 
nuget package to projects. Alternatively, they can be consumed from F# interactive 
by installing [`MBrace.Runtime`](http://www.nuget.org/packages/MBrace.Runtime) and loading
*)
#load "../packages/MBrace.Runtime/bootstrap.fsx"

open Nessos.MBrace
open Nessos.MBrace.Client
(**

## Overview

The MBrace client API that provides access to the following functionalities:

  1. The cloud workflow [programming model](progamming-model.html).

  2. An interface for managing and interacting with the MBrace runtime, that can 
     be roughly divided in the following categories:

      * Runtime administration functionality, that includes cluster management operations, 
        health monitoring and real-time elastic node management.

      * Cloud process management functionality, that includes submission of computations, 
        process monitoring, debugging and storage access.

  3. The MBrace shell, which enables interactive, on-the-fly declaration, 
     deployment and debugging of cloud computation through the F# REPL.

  4. A collection of command line tools for server-side deployments.

  5. A rich library of combinators implementing common parallelism workflows like MapReduce 
     or Choice and a multitude of sample implementations of real-world algorithms.

*)