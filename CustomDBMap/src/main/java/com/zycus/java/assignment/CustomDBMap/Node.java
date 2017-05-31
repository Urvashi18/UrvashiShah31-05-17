package com.zycus.java.assignment.CustomDBMap;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class Node implements Externalizable
{
    public int key;
    private long value;
    private long[] values;
    public Node left;
    public Node right;
    public Node parent;
    public int color;

    public Node() {
    }

    public void writeExternal(ObjectOutput objectOutput) throws IOException {
        objectOutput.writeInt(key);
        objectOutput.writeLong(value);
        if(values != null){
            objectOutput.writeInt(values.length);
            for (long l : values) {
                objectOutput.writeLong(l);
            }
        }else{
            objectOutput.writeInt(-1);
        }
        if(right != null){
            objectOutput.writeInt(1);
            right.writeExternal(objectOutput);
        }else {
            objectOutput.writeInt(-1);
        }
        if(left != null){
            objectOutput.writeInt(1);
            left.writeExternal(objectOutput);
        }else {
            objectOutput.writeInt(-1);
        }
    }

    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
        key = objectInput.readInt();
        value = objectInput.readLong();
        int valLength = objectInput.readInt();
        if(valLength > 0){
            values = new long[valLength];
            for (int i = 0; i<valLength; i++) {
                values[i] = objectInput.readLong();
            }
        }
        if(objectInput.readInt() == 1){
            right = new Node();
            right.readExternal(objectInput);
        }
        if(objectInput.readInt() == 1){
            left = new Node();
            left.readExternal(objectInput);
        }
    }

    public Node(int key, long value, int color, Node left, Node right) {
        this.key = key;
        this.value = value;
        this.color = color;
        this.left = left;
        this.right = right;
    }

    public Node grandparent() {
        assert parent != null; // Not the root node
        assert parent.parent != null; // Not child of root
        return parent.parent;
    }

    public Node sibling() {
        assert parent != null; // Root node has no sibling
        if (this == parent.left)
            return parent.right;
        else
            return parent.left;
    }

    public Node uncle() {
        assert parent != null; // Root node has no uncle
        assert parent.parent != null; // Children of root have no uncle
        return parent.sibling();
    }

    public long getValue() {
        return value;
    }

    public long[] getValues() {
        return values;
    }

    void addValue(long value) {
        if (values == null) {
            values = new long[]{value};
        } else {
            long[] temp = new long[values.length + 1];
            System.arraycopy(values, 0, temp, 0, values.length);
            temp[temp.length - 1] = value;
            values = temp;
        }
    }

    void deleteValue(long value) {
        int idx = -1;
        for (int i = 0; i < values.length; i++) {
            if (values[i] == value) {
                idx = i;
                break;
            }
        }
        if (idx == -1) {
            return;
        }
        long[] temp = new long[values.length - 1];
        if (idx == 0) {
            System.arraycopy(values, 1, temp, 0, values.length - 1);
        } else if (idx == temp.length - 1) {
            System.arraycopy(values, 0, temp, 0, values.length - 1);
        } else {
            System.arraycopy(values, 0, temp, 0, idx);
            System.arraycopy(values, idx + 1, temp, idx, values.length - (idx + 1));
        }
        values = temp;
    }

    void setValue(long value) {
        this.value = value;
    }

    void setValues(long[] values) {
        this.values = new long[values.length];
        System.arraycopy(this.values, 0, values, 0, values.length);
    }


    public static interface Color {
        public static final int RED = 1;
        public static final int BLACK = 2;
    }
}

