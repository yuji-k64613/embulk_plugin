package org.embulk.filter.myfilter;

import com.google.common.base.Optional;
import org.embulk.config.*;
import org.embulk.spi.*;

public class MyfilterFilterPlugin
        implements FilterPlugin
{
    public interface PluginTask
            extends Task
    {
        // configuration option 1 (required integer)
        @Config("option1")
        public int getOption1();

        // configuration option 2 (optional string, null is not allowed)
        @Config("option2")
        @ConfigDefault("\"myvalue\"")
        public String getOption2();

        // configuration option 3 (optional string, null is allowed)
        @Config("option3")
        @ConfigDefault("null")
        public Optional<String> getOption3();
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        Schema outputSchema = inputSchema;

        control.run(task.dump(), outputSchema);
    }

    @Override
    public PageOutput open(TaskSource taskSource, final Schema inputSchema,
                           final Schema outputSchema, final PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        // Write your code here :)
        //throw new UnsupportedOperationException("MyfilterFilterPlugin.open method is not implemented yet");

        return new PageOutput() {
            private final PageReader pageReader = new PageReader(inputSchema);
            private final PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);

            @Override
            public void finish() {
                pageBuilder.finish();
            }

            @Override
            public void close() {
                pageBuilder.close();
            }

            @Override
            public void add(Page page) {
                pageReader.setPage(page);

                while (pageReader.nextRecord()) {
                    inputSchema.visitColumns(new ColumnVisitor() {
                        @Override
                        public void booleanColumn(Column column) {
                            if (pageReader.isNull(column)) {
                                pageBuilder.setNull(column);
                            } else {
                                pageBuilder.setBoolean(column, pageReader.getBoolean(column));
                            }
                        }

                        @Override
                        public void longColumn(Column column) {
                            if (pageReader.isNull(column)) {
                                pageBuilder.setNull(column);
                            } else {
                                pageBuilder.setLong(column, pageReader.getLong(column));
                            }
                        }

                        @Override
                        public void doubleColumn(Column column) {
                            if (pageReader.isNull(column)) {
                                pageBuilder.setNull(column);
                            } else {
                                pageBuilder.setDouble(column, pageReader.getDouble(column));
                            }
                        }

                        @Override
                        public void stringColumn(Column column) {
                            if (pageReader.isNull(column)) {
                                pageBuilder.setNull(column);
                            } else {
//                                pageBuilder.setString(column, pageReader.getString(column));
                                String str = pageReader.getString(column);
                                pageBuilder.setString(column, str.toUpperCase());
                            }
                        }

                        @Override
                        public void timestampColumn(Column column) {
                            if (pageReader.isNull(column)) {
                                pageBuilder.setNull(column);
                            } else {
                                pageBuilder.setTimestamp(column, pageReader.getTimestamp(column));
                            }
                        }

                        @Override
                        public void jsonColumn(Column column) {
                            if (pageReader.isNull(column)) {
                                pageBuilder.setNull(column);
                            } else {
                                pageBuilder.setJson(column, pageReader.getJson(column));
                            }
                        }
                    });
                    pageBuilder.addRecord();
                }
            }
        };
    }
}
