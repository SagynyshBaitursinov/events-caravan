package io.saga.caravan.messaging;

import java.util.List;
import java.util.function.Consumer;

public interface DeleteMessages extends Consumer<List<Message>> {
}
