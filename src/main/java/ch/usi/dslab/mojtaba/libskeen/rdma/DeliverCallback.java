package ch.usi.dslab.mojtaba.libskeen.rdma;

import javafx.util.Pair;

public interface DeliverCallback {
    void call(Pair<Integer, Integer> deliverPair);
}
