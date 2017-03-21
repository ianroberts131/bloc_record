module BlocRecord
  class Collection < Array
    
    def update_all(updates)
      ids = self.map(&:id)
      
      self.any? ? self.first.class.update(ids, updates) : false
    end
    
    def destroy_all
      ids = self.map(&:id)
      if self.any?
        ids.each do |id|
          self.first.class.destroy(id)
        end
      else
        false
      end
    end
    
    def take(num=1)
      raise ArgumentError, "Argument is not numeric." unless num.is_a? Numeric
      raise ArgumentError, "Number must be greater than 0." unless num > 0
      array = []
      if num > 1
        num = self.length > num ? num : self.length
        (0..self.length-1).to_a.each do |index|
          array << self[index]
        end
        return array
      else
        return self[0]
      end
    end
    
    def where(args)
      array = []
      self.each do |item|
        array << item if item.send(args.keys[0]) == args.values[0]
      end
      array
    end
  end
end