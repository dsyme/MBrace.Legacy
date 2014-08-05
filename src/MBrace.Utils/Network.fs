namespace Nessos.MBrace.Utils

#nowarn "25" // Network.fs(42,17): warning FS0025: Incomplete pattern matches on this expression. For example, the value '[|_; _; _; _; _|]' may indicate a case not covered by the pattern(s).

module IPSubnet =
    open System
    open System.Net
    open System.Text.RegularExpressions
    
    let private parseIP ip = 
        match IPAddress.TryParse(ip) with
        | false, _ -> None 
        | true, ip -> Some ip

    let (|CIDR|_|) (subnet : string) =
        let tmp = subnet.Split('/')
        if tmp.Length <> 2 then 
            None
        else
            let subnet = parseIP tmp.[0] 
            let range =
                match Int32.TryParse(tmp.[1]) with
                | false, _ -> None
                | true, r  -> Some r
            match subnet, range with
            | None, _ 
            | _, None -> None
            | Some s, Some r -> Some (s, r)

    let contains (cidrSubnet : string) (ipAddress : string) =

        let ipAddr = 
            match parseIP ipAddress with 
            | Some ip -> ip 
            | _ -> invalidArg "ipAddress" "Invalid format"
        
        let subnetAddr, range =
            match cidrSubnet with
            | CIDR(s,r) -> s, r
            | _ -> invalidArg "cidrSubnet" "Invalid format"

        let addr_to_uint (addr : IPAddress) = 
            let [|a;b;c;d|] = addr.GetAddressBytes() |> Array.map uint64
            (a <<< 24) ||| (b <<< 16) ||| (c <<< 8) ||| d
        
        let ip = addr_to_uint ipAddr

        let minIp = addr_to_uint subnetAddr &&& (~~~ ((1uL <<< (32-range)) - 1uL))
        let maxIp = minIp + ((1uL <<< (32-range)) - 1uL) 

        minIp <= ip && ip <= maxIp