function count(stream)

    function mapper(rec)
        return 1
    end
    
    function reducer(v1, v2)
        return v1 + v2
    end

    return stream : map(mapper) : reduce(reducer)
end


function search(stream, exLevel, gender, minAge, maxAge)

    function framefilter(rec)
        if exLevel >= 0 then
            if rec.exLevel ~= exLevel then
                return false
            end
        end

        if rec.faceIds ~= null then
            for faceId in list.iterator(rec.faceIds) do
                if rec.genders ~= null and rec.ages ~= null and faceFilter(rec.genders[faceId], rec.ages[faceId]) then
                    return true
                end
            end
        end

        return false
    end


    function faceFilter(faceGender, faceAge)
        if gender > 0 then
            if faceGender ~= gender then
                return false
            end
        end

        if minAge > 0 then
            if faceAge == null then
                return false
            end
            if faceAge < minAge then
                return false
            end
            if faceAge > maxAge then
                return false
            end
        end

        return true
    end


    local function aggregate_by_streamId(itemMap, rec)
        local key = rec.streamId
        local storedItem = itemMap[key] 
        -- if storedItem == null then
        --     itemMap[key] = rec.streamId
        -- end
    
        if storedItem == null or storedItem.time < rec.time then
            itemMap[key] = map
            {
                streamId = rec.streamId,
                time = rec.time,
                url = rec.thumbnailUrl,
                faceIds = rec.faceIds,
                exLevel = rec.exLevel,
                ages = rec.ages,
                genders = rec.genders
            }
        end
    
        return itemMap
    end

    local function reduce_values(a, b)
        return map.merge(a, b, fn_merge)
    end
    
    local function fn_merge(a, b)
        return a
    end

    function getMapSize(rec)
        return map.size(rec)
    end
    
    return stream : filter(framefilter) : aggregate(map(), aggregate_by_streamId) : reduce(reduce_values) : map(getMapSize)
end
