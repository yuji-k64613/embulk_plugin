Embulk::JavaPlugin.register_filter(
  "myfilter", "org.embulk.filter.myfilter.MyfilterFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
