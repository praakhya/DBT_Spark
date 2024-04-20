import jdk.jshell.spi.ExecutionControl;
import org.apache.zookeeper.Op;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;

@Service
public class UserService {
    @Autowired
    private UserRepository userRepository;
    public void addUser (User user){
    }
    public Iterable<User> getUsers (){
        return new ArrayList<>();
    }
    public Optional<User> getUser (String id) {
        return Optional.of(new User());
    }
    public User editUser (User user)
            throws ExecutionControl.UserException{
        return new User();
    }
    public void deleteUser (String id) {
    }
    public boolean userExist (String id) {
        return true;
    }
}
