package dev.baitursinov.caravan.test;

import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@SpringBootTest(classes = SpringBootTestApplication.class)
@ExtendWith(SpringExtension.class)
public abstract class AbstractSpringBootTest {
}
