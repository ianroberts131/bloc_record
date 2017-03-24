require 'sqlite3'
require 'pg'

module Connection
  def connection
    if !@connection && BlocRecord.database_filename =~ /sqlite/
      @connection = SQLite3::Database.new(BlocRecord.database_filename)
    elsif !@connection && BlocRecord.database_filename =~ /pg/
      @connection = PG::connect( dbname: BlocRecord.database_filename )
    else
      @connection
    end
  end
end
