package com.example.distributedlockswithspringintegration;

import lombok.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.dao.DataAccessException;
import org.springframework.integration.jdbc.lock.DefaultLockRepository;
import org.springframework.integration.jdbc.lock.JdbcLockRegistry;
import org.springframework.integration.jdbc.lock.LockRepository;
import org.springframework.integration.support.locks.LockRegistry;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

@SpringBootApplication
public class Lox {

	public static void main(String[] args) {
		SpringApplication.run(Lox.class, args);
	}


	@Bean
	DefaultLockRepository defaultLockRepository(DataSource dataSource) {
		return new DefaultLockRepository(dataSource);
	}

	@Bean
	JdbcLockRegistry jdbcLockRegistry(LockRepository repository) {
		return new JdbcLockRegistry(repository);
	}

}

@RestController
@RequiredArgsConstructor
class LockedResourceRestController {

	private final LockRegistry registry;
	private final ReservationRepository reservationRepository;

	@SneakyThrows
	@GetMapping("/update/{id}/{name}/{time}")
	Reservation update(@PathVariable Integer id,
																				@PathVariable String name,
																				@PathVariable Long time) {

		String key = Integer.toString(id);
		Lock lock = registry.obtain(key);
		boolean lockAcquired = lock.tryLock(1, TimeUnit.SECONDS);
		if (lockAcquired) {
			try {
				doUpdateFor(id, name);
				Thread.sleep(time);
			}
			finally {
				lock.unlock();
			}
		}
		return reservationRepository.findById(id).get();
	}

	private void doUpdateFor(Integer id, String name) {
		reservationRepository
			.findById(id)
			.ifPresent(r -> {
				r.setName(name);
				reservationRepository.update(r);
			});

	}


}

@Repository
@RequiredArgsConstructor
class ReservationRepository {

	private final RowMapper<Reservation> rowMapper = (rs, i) -> new Reservation(rs.getInt("id"), rs.getString("name"));
	private final JdbcTemplate template;

	Optional<Reservation> findById(Integer id) {
		List<Reservation> reservations = template.query("select * from reservation where id = ? ", this.rowMapper, id);
		if (reservations.size() > 0) {
			return Optional.ofNullable(reservations.iterator().next());
		}
		return Optional.empty();
	}

	Reservation update(Reservation reservation) {
		Assert.isTrue(reservation.getId() != null && reservation.getId() != 0, "the id must be non-null");
		return template.execute("update reservation set name  = ? where id =? ", (PreparedStatementCallback<Reservation>) preparedStatement -> {
			preparedStatement.setString(1, reservation.getName());
			preparedStatement.setInt(2, reservation.getId());
			preparedStatement.execute();
			return findById(reservation.getId()).get();
		});
	}

	Collection<Reservation> findAll() {
		return template.query("select * from reservation", this.rowMapper);
	}
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class Reservation {
	private Integer id;
	private String name;
}
