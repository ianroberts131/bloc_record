require 'sqlite3'

module Selection
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
  
  private
  def init_object_from_row(row)
    if row
      data = Hash[columns.zip(row)]
      new(data)
    end
  end
  
  def rows_to_array(rows)
    rows.map { |row| new(Hash[columns.zip(row)]) }
  end
  
end