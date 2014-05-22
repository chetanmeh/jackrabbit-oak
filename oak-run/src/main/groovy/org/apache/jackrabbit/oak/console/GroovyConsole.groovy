/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.console

import groovy.transform.CompileStatic
import groovy.transform.TypeCheckingMode
import jline.Terminal
import jline.TerminalFactory
import jline.console.history.FileHistory
import org.apache.jackrabbit.oak.console.commands.CdCommand
import org.apache.jackrabbit.oak.console.commands.CheckpointCommand
import org.apache.jackrabbit.oak.console.commands.LsCommand
import org.apache.jackrabbit.oak.console.commands.LsdDocumentCommand
import org.apache.jackrabbit.oak.console.commands.OakHelpCommand
import org.apache.jackrabbit.oak.console.commands.PnCommand
import org.apache.jackrabbit.oak.console.commands.PrintDocumentCommand
import org.apache.jackrabbit.oak.console.commands.RefreshCommand
import org.apache.jackrabbit.oak.console.commands.RetrieveCommand
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore
import org.apache.jackrabbit.oak.run.Main
import org.codehaus.groovy.runtime.StackTraceUtils
import org.codehaus.groovy.tools.shell.ExitNotification
import org.codehaus.groovy.tools.shell.Groovysh
import org.codehaus.groovy.tools.shell.IO
import org.codehaus.groovy.tools.shell.InteractiveShellRunner
import org.codehaus.groovy.tools.shell.Interpreter
import org.codehaus.groovy.tools.shell.commands.AliasCommand
import org.codehaus.groovy.tools.shell.commands.ClearCommand
import org.codehaus.groovy.tools.shell.commands.DisplayCommand
import org.codehaus.groovy.tools.shell.commands.DocCommand
import org.codehaus.groovy.tools.shell.commands.EditCommand
import org.codehaus.groovy.tools.shell.commands.ExitCommand
import org.codehaus.groovy.tools.shell.commands.HistoryCommand
import org.codehaus.groovy.tools.shell.commands.ImportCommand
import org.codehaus.groovy.tools.shell.commands.InspectCommand
import org.codehaus.groovy.tools.shell.commands.LoadCommand
import org.codehaus.groovy.tools.shell.commands.PurgeCommand
import org.codehaus.groovy.tools.shell.commands.RecordCommand
import org.codehaus.groovy.tools.shell.commands.RegisterCommand
import org.codehaus.groovy.tools.shell.commands.SaveCommand
import org.codehaus.groovy.tools.shell.commands.SetCommand
import org.codehaus.groovy.tools.shell.commands.ShowCommand
import org.codehaus.groovy.tools.shell.util.Preferences

import org.codehaus.groovy.tools.shell.Command as ShellCommand

@CompileStatic
class GroovyConsole {
    private final List<String> args
    private final ConsoleSession session
    private final Groovysh shell
    private final boolean quiet;
    private final IO io = new IO();

    GroovyConsole(ConsoleSession session, List<String> args, boolean quiet) {
        this.args = args;
        this.session = session
        this.shell = prepareShell()
        this.quiet = quiet
    }

    int run(){
        return shell.run(args as String[])
    }

    int execute(List<String> args){
        try {
            shell.execute(args.join(' '))
            return 0;
        }catch(Throwable t){
            t.printStackTrace(new PrintStream(System.out));
            return 1;
        }
    }

    private Groovysh prepareShell() {
        Binding binding = new Binding(args as String[])
        binding['session'] = session
        if(quiet) {
            io.verbosity = IO.Verbosity.QUIET
        }
        Groovysh sh = new OakSh(getClass().getClassLoader(),
                binding, io, this.&registerCommands)
        sh.imports << 'org.apache.jackrabbit.oak.plugins.document.*'
        return sh
    }

    private void registerCommands(Groovysh shell){
        List<? extends ShellCommand> commands = []
        commands.addAll([
                new ExitCommand(shell),
                new ImportCommand(shell),
                new DisplayCommand(shell),
                new ClearCommand(shell),
                new ShowCommand(shell),
                new InspectCommand(shell),
                new PurgeCommand(shell),
                new EditCommand(shell),
                new LoadCommand(shell),
                new SaveCommand(shell),
                new RecordCommand(shell),
                new HistoryCommand(shell),
                new AliasCommand(shell),
                new SetCommand(shell),
                // does not do anything
                //new ShadowCommand(shell),
                new RegisterCommand(shell),
                new DocCommand(shell),
        ]);

        commands.addAll([
                //Oak Commands
                new OakHelpCommand(shell),
                new CdCommand(shell),
                new CheckpointCommand(shell),
                new LsCommand(shell),
                new PnCommand(shell),
                new RefreshCommand(shell),
                new RetrieveCommand(shell)
        ])

        if(session.store instanceof DocumentNodeStore){
            commands.addAll([
                    //Oak Commands
                    new PrintDocumentCommand(shell),
                    new LsdDocumentCommand(shell),
            ])
        }

        commands.each {ShellCommand command ->
            shell.register(command)
        }
    }

    private class OakSh extends Groovysh {
        private boolean colored = false

        OakSh(ClassLoader classLoader, Binding binding, IO io, Closure registrar) {
            super(classLoader, binding, io, registrar)
        }

        public String renderPrompt() {
            return prompt.render( buildPrompt() )
        }

        //Following methods are copied because they are private in parent however
        //they are referred via method handle which somehow looks for method in
        //derived class
        private String buildPrompt() {
            def prefix = session.workingPath
            def groovyshellProperty = System.getProperty("groovysh.prompt")
            def groovyshellEnv = System.getenv("GROOVYSH_PROMPT")
            if (groovyshellProperty) {
                prefix = groovyshellProperty
            } else if (groovyshellEnv) {
                prefix = groovyshellEnv
            }
            return colored ? "@|bold,blue ${prefix}>|@ " : "${prefix}>"
        }

        private void displayError(final Throwable cause) {
            if (errorHook == null) {
                throw new IllegalStateException("Error hook is not set")
            }
            if (cause instanceof MissingPropertyException) {
                if (cause.type && cause.type.canonicalName == Interpreter.SCRIPT_FILENAME) {
                    io.err.println("@|bold,red Unknown property|@: " + cause.property)
                    return
                }
            }

            errorHook.call(cause)
        }

        @CompileStatic(TypeCheckingMode.SKIP)
        private void maybeRecordError(Throwable cause) {
            def record = registry[RecordCommand.COMMAND_NAME]

            if (record != null) {
                boolean sanitize = Preferences.sanitizeStackTrace

                if (sanitize) {
                    cause = StackTraceUtils.deepSanitize(cause);
                }

                record.recordError(cause)
            }
        }

        @CompileStatic(TypeCheckingMode.SKIP)
        int run(final String commandLine) {
            Terminal term = TerminalFactory.create()
            colored = term.ansiSupported
            if (log.debug) {
                log.debug("Terminal ($term)")
                log.debug("    Supported:  $term.supported")
                log.debug("    ECHO:       (enabled: $term.echoEnabled)")
                log.debug("    H x W:      ${term.getHeight()} x ${term.getWidth()}")
                log.debug("    ANSI:       ${term.isAnsiSupported()}")

                if (term instanceof jline.WindowsTerminal) {
                    jline.WindowsTerminal winterm = (jline.WindowsTerminal) term
                    log.debug("    Direct:     ${winterm.directConsole}")
                }
            }

            def code

            try {
                loadUserScript('groovysh.profile')

                // if args were passed in, just execute as a command
                // (but cygwin gives an empty string, so ignore that)
                if (commandLine != null && commandLine.trim().size() > 0) {
                    // Run the given commands
                    execute(commandLine)
                } else {
                    loadUserScript('groovysh.rc')

                    // Setup the interactive runner
                    runner = new InteractiveShellRunner(
                            this,
                            this.&renderPrompt as Closure,
                            Integer.valueOf(Preferences.get(METACLASS_COMPLETION_PREFIX_LENGTH_PREFERENCE_KEY, '3')))

                    // Setup the history
                    File histFile = new File(userStateDirectory, 'groovysh.history')
                    history = new FileHistory(histFile)
                    runner.setHistory(history)

                    // Setup the error handler
                    runner.errorHandler = this.&displayError

                    //
                    // TODO: See if we want to add any more language specific completions, like for println for example?
                    //

                    // Display the welcome banner
                    if (!io.quiet) {
                        int width = term.getWidth()

                        // If we can't tell, or have something bogus then use a reasonable default
                        if (width < 1) {
                            width = 80
                        }

                        io.out.println("@|green Jackrabbit Oak Shell|@ (${Main.getProductInfo()}, " +
                                "JVM: ${System.properties['java.version']})")
                        io.out.println("Type '@|bold :help|@' or '@|bold :h|@' for help.")
                        io.out.println('-' * (width - 1))
                    }

                    // And let 'er rip... :-)
                    runner.run()
                }

                code = 0
            }
            catch (ExitNotification n) {
                log.debug("Exiting w/code: ${n.code}")

                code = n.code
            }
            catch (Throwable t) {
                io.err.println(messages.format('info.fatal', t))
                t.printStackTrace(io.err)

                code = 1
            }

            assert code != null // This should never happen

            return code
        }

    }
}
