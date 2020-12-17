package cp1.tests.helper;

import cp1.base.ResourceId;

// from the example
public class SimpleResourceId implements ResourceId {

    private int value;

    public SimpleResourceId(int value) {
        this.value = value;
    }

    @Override
    public int compareTo(ResourceId other) {
        if (! (other instanceof SimpleResourceId)) {
            throw new RuntimeException("Comparing incompatible resource IDs");
        }
        SimpleResourceId second = (SimpleResourceId)other;
        return Integer.compare(this.value, second.value);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SimpleResourceId)) {
            return false;
        }
        SimpleResourceId second = (SimpleResourceId)obj;
        return this.value == second.value;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(this.value);
    }

    @Override
    public String toString() {
        return "R" + this.value;
    }

}
