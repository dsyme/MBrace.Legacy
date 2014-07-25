namespace Nessos.MBrace.Runtime.Tests

    open NUnit.Framework

    /// Tests that need a runtime.
    type ClusterTestsCategoryAttribute() = 
        inherit CategoryAttribute("ClusterTests")

    /// Runtime administration tests (attach, reboot, etc).
    type RuntimeAdministrationCategoryAttribute() = 
        inherit CategoryAttribute("Runtime Administration")