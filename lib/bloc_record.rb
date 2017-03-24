module BlocRecord
  def self.connect_to(filename, platform)
    if platform.to_s == "sqlite3"
      @database_filename = "#{filename}.sqlite"
    elsif platform.to_s == "pg"
      @database_filename = "#{filename}"
    end
  end
  
  def self.database_filename
    @database_filename
  end
end