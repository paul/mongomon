#require 'mongo'
require 'mongo_mapper'

class Mongomon

  attr_reader :conn

  def initialize(options = {})
    @conn = Mongo::Connection.new(options[:hostname], options[:port])
    @options = options
    @options[:except_databases] ||= ["admin", "local"]
  end

  def hostname
    conn.host
  end

  def report!
    report = []
    time = Time.now.utc.to_i

    #database sizes
    conn.database_info.each do |db, size|
      unless @options[:except_databases].include?(db)
        report << format_database(db, time, size)
      end
    end

    databases.each do |db|
      collections(db).each do |collname|
        stats(db, collname).each do |name, val|
          report << format_collection_stat(db.name, collname, time, name, val)
        end
      end
    end

    report.join("\n")
  end

  protected

  def databases
    @conn.database_names.map do |name|
      if @options[:except_databases].include?(name)
        nil
      else
        @conn[name]
      end
    end.compact
  end

  def collections(db)
    db.collection_names.map do |collname|
      if collname =~ /^system\./
        nil
      else
        collname
      end
    end.compact
  end

  def stats(db, collname)
    db.command(:collstats => collname).delete_if { |k,v| k == "ns" }
  end

  def format_database(name, time, size)
    %{"#{hostname}/database/#{name}/sizeOnDisk",#{time},#{size}}
  end

  def format_collection_stat(db, coll, time, name, val)
    %{"#{hostname}/database/#{db}/collection/#{coll}/#{name}",#{time},#{val}}
  end

end
