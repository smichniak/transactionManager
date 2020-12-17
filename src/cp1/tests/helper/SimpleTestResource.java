package cp1.tests.helper;

import cp1.base.Resource;
import cp1.base.ResourceId;

import java.util.HashSet;
import java.util.Set;

public class SimpleTestResource extends Resource {

    public final Set<Object> activeOperations = new HashSet<>();

    public SimpleTestResource(ResourceId id) {
        super(id);
    }

}
