require 'sqlite3'

module Selection
  class WhereChain
    def initialize(scope)
      @scope = scope
    end
    
    def not(args)
      puts "The args are #{args.inspect}"
      puts "The scope is #{@scope}"
      if args.class == Hash
        string = "#{args.keys[0]} <> '#{args.values[0]}'"
      end
      puts "The string is #{string}"
      @scope.where(string)
    end
  end
  
  def find(*ids)
    original_ids = ids
    ids = ids.map { |id| id.to_i }
    ids.each { |id| ids.delete(id) unless id != 0 }
    
    if ids.length == 0
      raise ArgumentError, "No valid id's provided."
    elsif ids.length == 1
      find_one(ids.first)
    else
      rows = connection.execute <<-SQL
        SELECT #{columns.join ","} FROM #{table}
        WHERE id IN (#{ids.join(",")});
      SQL
    
      if rows.length == 0
        raise ArgumentError, "No ids in #{original_ids.inspect} found"
      else
        rows_to_array(rows)
      end
    end
  end
  
  def find_one(id)
    raise ArgumentError, "id #{id} is not a valid input." unless id.to_i != 0
    row = connection.get_first_row <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      WHERE id = #{id};
    SQL
    
    if row.length == 0
      raise ArgumentError, "id #{id} not found."
    else
      init_object_from_row(row)
    end
  end
  
  def find_by(attribute, value)
    raise ArgumentError, "Attribute #{attribute} not found." unless columns.include?(attribute.to_s)
    row = connection.get_first_row <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      WHERE #{attribute} = #{BlocRecord::Utility.sql_strings(value)};
    SQL
    
    if row.length == 0
      raise ArgumentError, "Attribute '#{attribute}' with value '#{value}' not found."
    else
      init_object_from_row(row)
    end
  end
  
  def method_missing(method_name, *args, &block)
    if method_name.to_s =~ /^find_by_(.*)/
      find_by($1.to_sym, args.first)
    else
      super
    end
  end
  
  def take(num=1)
    raise ArgumentError, "Argument is not numeric." unless num.is_a? Numeric
    if num > 1
      rows = connection.execute <<-SQL
        SELECT #{columns.join ","} FROM #{table}
        ORDER_BY random()
        LIMIT #{num};
      SQL
      
      rows_to_array(rows)
    else
      take_one
    end
  end
  
  def take_one
    row = connection.get_first_row <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      ORDER BY random()
      LIMIT 1;
    SQL
    
    init_object_from_row(row)
  end
  
  def first
    row = connection.get_first_row <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      ORDER BY id
      ASC LIMIT 1;
    SQL
    
    init_object_from_row(row)
  end
  
  def last
    row = connection.get_first_row <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      ORDER BY id
      DESC LIMIT 1;
    SQL
    
    init_object_from_row(row)
  end
  
  def all
    rows = connection.execute <<-SQL
      SELECT #{columns.join ","} FROM #{table};
    SQL
    
    rows_to_array(rows)
  end
  
  def find_each(start:, batch_size:)
    last_element = start + batch_size
    item = start
    while item < last_element
      yield self.find_one(item)
      item += 1
    end
  end
  
  def find_in_batches(start:, batch_size:)
    rows = connection.execute <<-SQL
    SELECT #{columns.join ","} FROM #{table} LIMIT #{batch_size} OFFSET #{start}
    SQL
    array = rows_to_array(rows)
    yield array
  end
  
  def where(*args)
    if args.count == 0
      puts "I'M HERE!"
      return WhereChain.new(self)
    end
    if args.count > 1
      expression = args.shift
      params = args
    else
      case args.first
      when String
        expression = args.first
      when Hash
        expression_hash = BlocRecord::Utility.convert_keys(args.first)
        expression = expression_hash.map { |key, value| "#{key}=#{BlocRecord::Utility.sql_strings(value)}" }.join(" and ")
      end
    end
    
    if self.class == Array
      sql = <<-SQL
        #{self[1]} WHERE #{expression};
      SQL
    else
      sql = <<-SQL
        SELECT #{columns.join ","} FROM #{table}
        WHERE #{expression};
      SQL
    end
    puts "The expression is #{expression}"
    puts "The SQL is #{sql}"
    
    rows = connection.execute(sql, params)
    rows_to_array(rows)
  end
  
  def order(*args)
    if args.count > 1
      args.each_with_index do |arg, index|
        if arg.class == Hash
          args[index] = arg.map { |key, value| "#{key} #{value}" }
        end
      end
      order = args.join(",")
    else
      if args.first.class == Hash
        array = []
        args[0].each { |key, value| array << "#{key} #{value}" }
        args[0] = array.join(",")
      end
      order = args.first.to_s
    end
    rows = connection.execute <<-SQL
      SELECT * FROM #{table}
      ORDER BY #{order}
    SQL
    rows_to_array(rows)
  end
  
  def join(*args)
    if args.count > 1
      joins = args.map { |arg| "INNER JOIN #{arg} ON #{arg}_#{table}_id = #{table}.id" }.join(" ")
      rows = connection.execute <<-SQL
        SELECT * FROM #{table} #{joins}
      SQL
    else
      if args.first.class == Hash
        join1, join2 = "", ""
        args.first.each do |key, value|
          join1 += "INNER JOIN #{key} ON #{key}.#{table}_id = #{table}.id"
          join2 += "INNER JOIN #{value} ON #{value}.#{key}_id = #{key}.id"
        end
        sql = <<-SQL
          SELECT * FROM #{table} #{join1} #{join2}
        SQL
        rows = connection.execute(sql)
      else
        case args.first
        when String
          sql = <<-SQL
          SELECT * FROM #{table} #{BlocRecord::Utility.sql_strings(args.first)};
          SQL
          rows = connection.execute(sql)
        when Symbol
          sql = <<-SQL
          SELECT * FROM #{table}
          INNER JOIN #{args.first} ON #{args.first}.#{table}_id = #{table}.id
          SQL
          rows = connection.execute(sql)
        end
      end
    end
   
    return rows_to_array(rows), sql
  end
  
  private
  def init_object_from_row(row)
    if row
      data = Hash[columns.zip(row)]
      new(data)
    end
  end
  
  def rows_to_array(rows)
    collection = BlocRecord::Collection.new
    rows.each { |row| collection << new(Hash[columns.zip(row)]) }
    collection
  end
  
end