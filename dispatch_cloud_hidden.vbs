Option Explicit
Dim sh, fso, logDir, logFile, cmd, result
Set sh = CreateObject("WScript.Shell")
sh.Environment("PROCESS")("GH_PROMPT_DISABLED") = "1"
cmd = """C:\Program Files\GitHub CLI\gh.exe"" workflow run publish.yml -R qudous44/qudus-alt-reel-runner"
result = sh.Run(cmd, 0, True)
On Error Resume Next
Set fso = CreateObject("Scripting.FileSystemObject")
logDir = sh.ExpandEnvironmentStrings("%LOCALAPPDATA%") & "\QudusAltReels"
If Not fso.FolderExists(logDir) Then fso.CreateFolder logDir
Set logFile = fso.OpenTextFile(logDir & "\dispatch.log", 8, True)
logFile.WriteLine Now & " exit=" & result
logFile.Close
On Error GoTo 0
WScript.Quit result
