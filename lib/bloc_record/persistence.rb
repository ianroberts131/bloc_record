require 'sqlite3'
require 'bloc_record/schema'

module Persistence
  def self.included(base)
    base.extend(ClassMethods)
  end
  
  def save
    self.save! rescue false
  end
  
  def save!
    unless self.id
      self.id = self.class.create(BlocRecord::Utility.instance_variable_to_hash(self)).id
      BlocRecord::Utility.reload_obj(self)
      return true
    end
    fields = self.class.attributes.map { |col| "#{col}=#{BlocRecord::Utility.sql_strings(self.instance_variable_get("@#{col}"))}" }.join(",")
    
    self.class.connection.execute <<-SQL
      UPDATE #{self.class.table}
      SET #{fields}
      WHERE id = #{self.id};
    SQL
    
    true
  end
  
  def method_missing(method_name, *args, &block)
    if method_name.to_s =~ /^update_(.*)/
      self.class.update(self.id, { $1 => args.first })
    else
      super
    end
  end
  
  def update_attribute(attribute, value)
    self.class.update(self.id, { attribute => value })
  end
  
  def update_attributes(updates)
    self.class.update(self.id, updates)
  end
  
  module ClassMethods
    def update_all(updates)
      update(nil, updates)
    end
    
    def create(attrs)
      attrs = BlocRecord::Utility.convert_keys(attrs)
      attrs.delete "id"
      vals = attributes.map { |key| BlocRecord::Utility.sql_strings(attrs[key]) }
      
      sql = <<-SQL
        INSERT INTO #{table} (#{attributes.join ","})
        VALUES (#{vals.join ","});
      SQL
      
      connection.execute sql
      
      data = Hash[attributes.zip attrs.values]
      data["id"] = connection.execute("SELECT last_insert_rowid();")[0][0]
      new(data)
    end
    
    def update(ids, updates)
      
      if updates.class == Array
        updates = updates.map { |hash| "'#{hash.keys[0]}'='#{hash.values[0]}'" }
        updates_hash = Hash[updates.zip ids]
        updates_array = updates_hash.map { |update, id| "UPDATE #{table} SET #{update} WHERE id = #{id};" }

        connection.execute_batch <<-SQL
          #{updates_array * " "}
        SQL
      else
        updates = BlocRecord::Utility.convert_keys(updates)
        
        updates.delete "id"
        
        updates_array = updates.map { |key, value| "#{key}=#{BlocRecord::Utility.sql_strings(value)}" }
        
        if ids.class == Fixnum
          where_clause = "WHERE id = #{ids};"
        elsif ids.class == Array
          where_clause = ids.empty? ? ";" : "WHERE id IN (#{ids.join(",")});"
        else
          where_clause = ";"
        end
      
        connection.execute <<-SQL
          UPDATE #{table}
          SET #{updates_array * ","} #{where_clause}
        SQL
      end
      
      true
    end
  end
end