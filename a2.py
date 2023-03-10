"""CSC343 Assignment 2

=== CSC343 Winter 2023 ===
Department of Computer Science,
University of Toronto

This code is provided solely for the personal and private use of
students taking the CSC343 course at the University of Toronto.
Copying for purposes other than this use is expressly prohibited.
All forms of distribution of this code, whether as given or with
any changes, are expressly prohibited.

Authors: Danny Heap, Marina Tawfik, and Jacqueline Smith

All of the files in this directory and all subdirectories are:
Copyright (c) 2023 Danny Heap and Jacqueline Smith

=== Module Description ===

This file contains the WasteWrangler class and some simple testing functions.

Group Members:
Shilei (Andy) Zhao, #1006867491
Lujie (Adam) Zhao, #1006879086
"""

# Andy - ODD
# Adam - EVEN
# (according to the a2.pdf handout in the function definition)

import datetime as dt
import psycopg2 as pg
import psycopg2.extensions as pg_ext
import psycopg2.extras as pg_extras
from typing import Optional, TextIO


# helper functions

# function that takes in two dt.date variables
# Returns true if the dates are on different days
# checks if the first date starts 30 minutes before the second date or 
# ends 30 minutes after the second date
# returns true if this is not the case, false otherwise
# also checks if time is within working hours (8am - 4pm)
def valid_truck_time(truck_time: dt.datetime, given_time: dt.datetime, length: int) -> bool:
    # print("in valid_truck_time()")
    
    if (given_time.date() != truck_time.date()): # on different days
        return True
    
    if (given_time.time() < dt.time(8, 0)): # if the given_time starts before 8am (too early for workday)
        return False
    
    else: # truck_time is on the same day as given_time
        time_diff = 0

        if (given_time.time() > truck_time.time()): # if truck_time is before given_time
            time_diff = given_time - truck_time

            if (time_diff.total_seconds() <= 1800): # this is a INVALID time (1800s = 30min)
                return False

        else: # if truck_time is after given_time
            route_time = length / 5 # [in hours] (assume all truck speeds = 5km/h)
            end_time = given_time + dt.timedelta(hours=int(route_time))
            
            if (end_time.time() > dt.time(16, 0)): # if the end_time goes after 16:00 (too late for workday)
                return False
            
            if (end_time.time() < truck_time.time()): # if ending time of the delivery is BEFORE the truck_time
                time_diff = truck_time - end_time
                
                if (time_diff.total_seconds() <= 1800): # if truck_time is within 30 min after delivery is finished
                    return False
                
            else: # if ending time of the delivery is AFTER the truck_time
                return False

        return True


class WasteWrangler:
    """A class that can work with data conforming to the schema in
    waste_wrangler_schema.ddl.

    === Instance Attributes ===
    connection: connection to a PostgreSQL database of a waste management
    service.

    Representation invariants:
    - The database to which connection is established conforms to the schema
      in waste_wrangler_schema.ddl.
    """
    connection: Optional[pg_ext.connection]

    def __init__(self) -> None:
        """Initialize this WasteWrangler instance, with no database connection
        yet.
        """
        self.connection = None

    def connect(self, dbname: str, username: str, password: str) -> bool:
        """Establish a connection to the database <dbname> using the
        username <username> and password <password>, and assign it to the
        instance attribute <connection>. In addition, set the search path
        to waste_wrangler.

        Return True if the connection was made successfully, False otherwise.
        I.e., do NOT throw an error if making the connection fails.

        >>> ww = WasteWrangler()
        >>> ww.connect("csc343h-marinat", "marinat", "")
        True
        >>> # In this example, the connection cannot be made.
        >>> ww.connect("invalid", "nonsense", "incorrect")
        False
        """
        try:
            self.connection = pg.connect(
                dbname=dbname, user=username, password=password,
                options="-c search_path=waste_wrangler"
            )
            return True
        except pg.Error:
            return False

    def disconnect(self) -> bool:
        """Close this WasteWrangler's connection to the database.

        Return True if closing the connection was successful, False otherwise.
        I.e., do NOT throw an error if closing the connection failed.

        >>> ww = WasteWrangler()
        >>> ww.connect("csc343h-marinat", "marinat", "")
        True
        >>> ww.disconnect()
        True
        """
        try:
            if self.connection and not self.connection.closed:
                self.connection.close()
            return True
        except pg.Error:
            return False

    def schedule_trip(self, rid: int, time: dt.datetime) -> bool:
        """Schedule a truck and two employees to the route identified
        with <rid> at the given time stamp <time> to pick up an
        unknown volume of waste, and deliver it to the appropriate facility.

        The employees and truck selected for this trip must be available:
            * They can NOT be scheduled for a different trip from 30 minutes
              of the expected start until 30 minutes after the end time of this
              trip.
            * The truck can NOT be scheduled for maintenance on the same day.

        The end time of a trip can be computed by assuming that all trucks
        travel at an average of 5 kph.

        From the available trucks, pick a truck that can carry the same
        waste type as <rid> and give priority based on larger capacity and
        use the ascending order of ids to break ties.

        From the available employees, give preference based on hireDate
        (employees who have the most experience get priority), and order by
        ascending order of ids in case of ties, such that at least one
        employee can drive the truck type of the selected truck.

        Pick a facility that has the same waste type a <rid> and select the one
        with the lowest fID.

        Return True iff a trip has been scheduled successfully for the given
            route.
        This method should NOT throw an error i.e. if scheduling fails, the
        method should simply return False.

        No changes should be made to the database if scheduling the trip fails.

        Scheduling fails i.e., the method returns False, if any of the following
        is true:
            * If rid is an invalid route ID.
            * If no appropriate truck, drivers or facility can be found.
            * If a trip has already been scheduled for <rid> on the same day
              as <time> (that encompasses the exact same time as <time>).
            * If the trip can't be scheduled within working hours i.e., between
              8:00-16:00.

        While a realistic use case will provide a <time> in the near future, our
        tests could use any valid value for <time>.
        """
        cur = self.connection.cursor()
        cur.execute('begin;')
        cur.execute('savepoint sp_schedule_trip;')

        try:
            # obtaining desired wasteType for the route
            r_wasteType = ""
            cur.execute(f'select wastetype from Route where rid={rid};')
            for row in cur:
                r_wasteType = row[0]
            # print(r_wasteType)
        
            
            r_drivers = {} # {<eid>: <hiredate>}
            r_trucks = {} # {<tid>: <capacity>}

            # filters out employees that are not drivers
            cur.execute(f'select distinct eid, hiredate from driver natural join employee;')
            for row in cur:
                r_drivers[row[0]] = row[1]
                
            # filters out trucks that cannot handle r_wasteType and trucks scheduled for a maintenance on the same day
            cur.execute(f'select tid, capacity from truck natural join trucktype natural join maintenance where mdate<>\'{time.date()}\' and wastetype=\'{r_wasteType}\';')
            for row in cur:
                r_trucks[row[0]] = int(row[1])

            # print(r_drivers)
            # print(r_trucks)


            # Trying to filter out any trucks OR drivers that are busy (i.e. scheduled on a trip during the same time as the given time)
            # filters out trucks that are scheduled for maintenance on the same day, matched with their routes, and filters invalid rID's
            cur.execute('select t.tid, eid1, eid2, ttime, length, t.rid from trip t, maintenance m, Route r where t.tid=m.tid and date(ttime)<>mdate and t.rid=r.rid;')
            # print(f"given time: {time}\n")
            
            for row in cur:
                tid = row[0]
                eid1 = row[1]
                eid2 = row[2]
                truck_time = row[3]
                length = row[4]
                rid_prime = row[5]
                # print(f"truck_time: {truck_time}")
                
                # if a trip has already been scheduled for this rid on the same day
                if (rid == rid_prime and time.date() == truck_time.date()):
                    return False
            
                # print(f"truck time: {truck_time}, given_time: {time}")
                if (not valid_truck_time(truck_time, time, length)):
                    # invalid time, removing invalid drivers and trucks
                    r_drivers.pop(eid1, None)
                    r_drivers.pop(eid2, None)
                    r_trucks.pop(tid, None)
                    

                # print()
        
            # print(r_drivers)
            # print(r_trucks)
            # print("\n\n")
            
            # if no suitable drivers or trucks
            if (len(r_drivers) < 2 or len(r_trucks) == 0):
                return False
            
            # picking a facility with matching r_wastetype
            final_facility = -1
            cur.execute(f'select fid from facility where wastetype=\'{r_wasteType}\';')
            for row in cur:
                final_facility = row[0]
                break
            if (final_facility == -1): # edge case: if no suitable facility
                return False
            
            
            final_eid1 = -1
            final_eid2 = -1
            at_least_one_wasteType = False
            oldest = dt.datetime(9999, 12, 31) # initialzie to date 9999-12-31
            
            # for eid1 - always the oldest hire date
            final_eid1 = min(r_drivers, key=lambda k: r_drivers[k])
            del r_drivers[final_eid1]
            
            # check if this employee can drive the wasteType
            cur.execute(f'select exists (select * from driver natural join trucktype where eid={final_eid1} and wastetype=\'{r_wasteType}\');')
            for row in cur:
                if (row[0] == True):
                    at_least_one_wasteType = True

            if (at_least_one_wasteType):
                # eid2 just has to be second oldest 
                final_eid2 = min(r_drivers, key=lambda k: r_drivers[k]) 
            else:
                # eid2 has to be the oldest driver that matches the wasteType
                sorted_drivers = sorted(r_drivers, key=lambda k: r_drivers[k]) # sort the dict into a list based on hiredate 
                for i in sorted_drivers:
                    cur.execute(f'select exists (select * from driver natural join trucktype where eid={i} and wastetype=\'{r_wasteType}\');')
                    for row in cur:
                        if (row[0] == True):
                            final_eid2 = i
                    if (final_eid2 != -1):
                        break
            

            final_tid = -1
            final_cap = -1
            for key, value in r_trucks.items(): # sort by capacity {<tid>: <capacity>}
                if (value == final_cap): # edge case: prioritize smaller tid #
                    final_tid = min(final_tid, key)
                elif (value > final_cap):
                    final_cap = value
                    final_tid = key

                
            cur.execute(f'insert into Trip values ({rid}, {final_tid}, \'{time}\', NULL, {final_eid1}, {final_eid2}, {final_facility});')    
            cur.execute('commit;')

            cur.close()
            return True
            
        except pg.Error as ex:
            # You may find it helpful to uncomment this line while debugging,
            # as it will show you all the details of the error that occurred:
            cur.execute('rollback to sp_schedule_trip;')
            cur.close()
            raise ex
            # pass
            # return False
        
            

    def schedule_trips(self, tid: int, date: dt.date) -> int:
        """Schedule the truck identified with <tid> for trips on <date> using
        the following approach:

            1. Find routes not already scheduled for <date>, for which <tid>
               is able to carry the waste type. Schedule these by ascending
               order of rIDs.

            2. Starting from 8 a.m., find the earliest available pair
               of drivers who are available all day. Give preference
               based on hireDate (employees who have the most
               experience get priority), and break ties by choosing
               the lower eID, such that at least one employee can
               drive the truck type of <tid>.

               The facility for the trip is the one with the lowest fID that can
               handle the waste type of the route.

               The volume for the scheduled trip should be null.

            3. Continue scheduling, making sure to leave 30 minutes between
               the end of one trip and the start of the next, using the
               assumption that <tid> will travel an average of 5 kph.
               Make sure that the last trip will not end after 4 p.m.

        Return the number of trips that were scheduled successfully.

        Your method should NOT raise an error.

        While a realistic use case will provide a <date> in the near future, our
        tests could use any valid value for <date>.
        """
        # TODO: implement this method
        pass

    def update_technicians(self, qualifications_file: TextIO) -> int:
        """Given the open file <qualifications_file> that follows the format
        described on the handout, update the database to reflect that the
        recorded technicians can now work on the corresponding given truck type.

        For the purposes of this method, you may assume that no two employees
        in our database have the same name i.e., an employee can be uniquely
        identified using their name.

        Your method should NOT throw an error.
        Instead, only correct entries should be reflected in the database.
        Return the number of successful changes, which is the same as the number
        of valid entries.
        Invalid entries include:
            * Incorrect employee name.
            * Incorrect truck type.
            * The technician is already recorded to work on the corresponding
              truck type.
            * The employee is a driver.

        Hint: We have provided a helper _read_qualifications_file that you
            might find helpful for completing this method.
        """
        try:
            # TODO: implement this method
            pass
        except pg.Error as ex:
            # You may find it helpful to uncomment this line while debugging,
            # as it will show you all the details of the error that occurred:
            # raise ex
            return 0

    def workmate_sphere(self, eid: int) -> list[int]:
        """Return the workmate sphere of the driver identified by <eid>, as a
        list of eIDs.

        The workmate sphere of <eid> is:
            * Any employee who has been on a trip with <eid>.
            * Recursively, any employee who has been on a trip with an employee
              in <eid>'s workmate sphere is also in <eid>'s workmate sphere.

        The returned list should NOT include <eid> and should NOT include
        duplicates.

        The order of the returned ids does NOT matter.

        Your method should NOT return an error. If an error occurs, your method
        should simply return an empty list.
        """
        try:
            # TODO: implement this method
            pass
        except pg.Error as ex:
            # You may find it helpful to uncomment this line while debugging,
            # as it will show you all the details of the error that occurred:
            # raise ex
            return []

    def schedule_maintenance(self, date: dt.date) -> int:
        """For each truck whose most recent maintenance before <date> happened
        over 90 days before <date>, and for which there is no scheduled
        maintenance up to 10 days following date, schedule maintenance with
        a technician qualified to work on that truck in ascending order of tIDs.

        For example, if <date> is 2023-05-02, then you should consider trucks
        that had maintenance before 2023-02-01, and for which there is no
        scheduled maintenance from 2023-05-02 to 2023-05-12 inclusive.

        Choose the first day after <date> when there is a qualified technician
        available (not scheduled to maintain another truck that day) and the
        truck is not scheduled for a trip or maintenance on that day.

        If there is more than one technician available on a given day, choose
        the one with the lowest eID.

        Return the number of trucks that were successfully scheduled for
        maintenance.

        Your method should NOT throw an error.

        While a realistic use case will provide a <date> in the near future, our
        tests could use any valid value for <date>.
        """
        try:
            # TODO: implement this method
            pass
        except pg.Error as ex:
            # You may find it helpful to uncomment this line while debugging,
            # as it will show you all the details of the error that occurred:
            # raise ex
            return 0

    def reroute_waste(self, fid: int, date: dt.date) -> int:
        """Reroute the trips to <fid> on day <date> to another facility that
        takes the same type of waste. If there are many such facilities, pick
        the one with the smallest fID (that is not <fid>).

        Return the number of re-routed trips.

        Don't worry about too many trips arriving at the same time to the same
        facility. Each facility has ample receiving facility.

        Your method should NOT return an error. If an error occurs, your method
        should simply return 0 i.e., no trips have been re-routed.

        While a realistic use case will provide a <date> in the near future, our
        tests could use any valid value for <date>.

        Assume this happens before any of the trips have reached <fid>.
        """
        try:
            # TODO: implement this method
            pass
        except pg.Error as ex:
            # You may find it helpful to uncomment this line while debugging,
            # as it will show you all the details of the error that occurred:
            # raise ex
            return 0

    # =========================== Helper methods ============================= #

    @staticmethod
    def _read_qualifications_file(file: TextIO) -> list[list[str, str, str]]:
        """Helper for update_technicians. Accept an open file <file> that
        follows the format described on the A2 handout and return a list
        representing the information in the file, where each item in the list
        includes the following 3 elements in this order:
            * The first name of the technician.
            * The last name of the technician.
            * The truck type that the technician is currently qualified to work
              on.

        Pre-condition:
            <file> follows the format given on the A2 handout.
        """
        result = []
        employee_info = []
        for idx, line in enumerate(file):
            if idx % 2 == 0:
                info = line.strip().split(' ')[-2:]
                fname, lname = info
                employee_info.extend([fname, lname])
            else:
                employee_info.append(line.strip())
                result.append(employee_info)
                employee_info = []

        return result


def setup(dbname: str, username: str, password: str, file_path: str) -> None:
    """Set up the testing environment for the database <dbname> using the
    username <username> and password <password> by importing the schema file
    and the file containing the data at <file_path>.
    """
    connection, cursor, schema_file, data_file = None, None, None, None
    try:
        # Change this to connect to your own database
        connection = pg.connect(
            dbname=dbname, user=username, password=password,
            options="-c search_path=waste_wrangler"
        )
        cursor = connection.cursor()

        schema_file = open("./waste_wrangler_schema.sql", "r")
        cursor.execute(schema_file.read())

        data_file = open(file_path, "r")
        cursor.execute(data_file.read())

        connection.commit()
    except Exception as ex:
        connection.rollback()
        raise Exception(f"Couldn't set up environment for tests: \n{ex}")
    finally:
        if cursor and not cursor.closed:
            cursor.close()
        if connection and not connection.closed:
            connection.close()
        if schema_file:
            schema_file.close()
        if data_file:
            data_file.close()


def test_preliminary() -> None:
    """Test preliminary aspects of the A2 methods."""
    ww = WasteWrangler()
    qf = None
    try:
        # TODO: Change the values of the following variables to connect to your
        #  own database:
        dbname = 'csc343h-zhaosh67'
        user = 'zhaosh67'
        password = ''

        connected = ww.connect(dbname, user, password)

        # The following is an assert statement. It checks that the value for
        # connected is True. The message after the comma will be printed if
        # that is not the case (connected is False).
        # Use the same notation to thoroughly test the methods we have provided
        assert connected, f"[Connected] Expected True | Got {connected}."

        # TODO: Test one or more methods here, or better yet, make more testing
        #   functions, with each testing a different aspect of the code.

        # The following function will set up the testing environment by loading
        # the sample data we have provided into your database. You can create
        # more sample data files and use the same function to load them into
        # your database.
        # Note: make sure that the schema and data files are in the same
        # directory (folder) as your a2.py file.
        setup(dbname, user, password, './waste_wrangler_data.sql')

        # --------------------- Testing schedule_trip  ------------------------#

        # You will need to check that data in the Trip relation has been
        # changed accordingly. The following row would now be added:
        # (1, 1, '2023-05-04 08:00', null, 2, 1, 1)
        scheduled_trip = ww.schedule_trip(1, dt.datetime(2023, 5, 4, 8, 0))
        assert scheduled_trip, \
            f"[Schedule Trip] Expected True, Got {scheduled_trip}"

        # Can't schedule the same route of the same day.
        scheduled_trip = ww.schedule_trip(1, dt.datetime(2023, 5, 4, 13, 0))
        assert not scheduled_trip, \
            f"[Schedule Trip] Expected False, Got {scheduled_trip}"

        # -------------------- Testing schedule_trips  ------------------------#

        # All routes for truck tid are scheduled on that day
        scheduled_trips = ww.schedule_trips(1, dt.datetime(2023, 5, 3))
        assert scheduled_trips == 0, \
            f"[Schedule Trips] Expected 0, Got {scheduled_trips}"

        # ----------------- Testing update_technicians  -----------------------#

        # This uses the provided file. We recommend you make up your custom
        # file to thoroughly test your implementation.
        # You will need to check that data in the Technician relation has been
        # changed accordingly
        qf = open('qualifications.txt', 'r')
        updated_technicians = ww.update_technicians(qf)
        assert updated_technicians == 2, \
            f"[Update Technicians] Expected 2, Got {updated_technicians}"

        # ----------------- Testing workmate_sphere ---------------------------#

        # This employee doesn't exist in our instance
        workmate_sphere = ww.workmate_sphere(2023)
        assert len(workmate_sphere) == 0, \
            f"[Workmate Sphere] Expected [], Got {workmate_sphere}"

        workmate_sphere = ww.workmate_sphere(3)
        # Use set for comparing the results of workmate_sphere since
        # order doesn't matter.
        # Notice that 2 is added to 1's work sphere because of the trip we
        # added earlier.
        assert set(workmate_sphere) == {1, 2}, \
            f"[Workmate Sphere] Expected {{1, 2}}, Got {workmate_sphere}"

        # ----------------- Testing schedule_maintenance ----------------------#

        # You will need to check the data in the Maintenance relation
        scheduled_maintenance = ww.schedule_maintenance(dt.date(2023, 5, 5))
        assert scheduled_maintenance == 7, \
            f"[Schedule Maintenance] Expected 7, Got {scheduled_maintenance}"

        # ------------------ Testing reroute_waste  ---------------------------#

        # There is no trips to facility 1 on that day
        reroute_waste = ww.reroute_waste(1, dt.date(2023, 5, 10))
        assert reroute_waste == 0, \
            f"[Reroute Waste] Expected 0. Got {reroute_waste}"

        # You will need to check that data in the Trip relation has been
        # changed accordingly
        reroute_waste = ww.reroute_waste(1, dt.date(2023, 5, 3))
        assert reroute_waste == 1, \
            f"[Reroute Waste] Expected 1. Got {reroute_waste}"
    finally:
        if qf and not qf.closed:
            qf.close()
        ww.disconnect()


if __name__ == '__main__':
    # Un comment-out the next two lines if you would like to run the doctest
    # examples (see ">>>" in the methods connect and disconnect)
    # import doctest
    # doctest.testmod()

    # TODO: Put your testing code here, or call testing functions such as
    #   this one:
    test_preliminary()
