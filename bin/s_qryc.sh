# File - s_qryc.sh
#
# Purpose - Bourne Shell emulation of NWIS S_QRYC() Fortran subroutine.
#
# Authors - Andy Halper <ashalper@usgs.gov> [Bourne Shell translation]
#           Scott Bartholoma <sbarthol@usgs.gov> [S_QRYC()]
#           Jeffrey D. Christman <jdchrist@usgs.gov> [S_QRYC()]
#

# Description from Fortran source:
#
# THIS SUBROUTINE GETS A CHARACTER STRING A RESPONSE OF "HELP" OR "?"
# WILL DISPLAY THE LEVEL 1 HELP, "OOPS" WILL TRIGGER THE ALTERNATE
# RETURN, "EXIT" WILL END THE PROGRAM, SETTING THE GLOBAL VARIABLE
# .EXIT TO TRUE, INDICATING EXIT TO PRIMOS, "QUIT" WILL END THE
# PROGRAM, SETTING THE GLOBAL VARIABLE .EXIT TO FALSE, INDICATING EXIT
# TO MENU.
s_qryc ()
{
    qmesge=$1      # Prompt message (wy prompt supplied by subroutine)
    iopt1=$2       # Help identification, blank for no help
    iopt2=$3       # Operations code 2, 0=no oops, 1=oops allowed
    iopt3=$4       # Operations code 3,
                   # 0=No retention of current value on carriage return,
                   # 1=Retain current value on carriage return
    inlnmn=$5      # Minimum characters allowed in answer
    inlnmx=$6      # Maximum characters allowed in answer
    otchrv=$7      # Output character value

#
# DISPLAY QUERY
   10 CALL S_MSGD (QMESGE)
#
# GET REPLY
 11   REPLY=' '
      read (5,'(A)',ERR=20, IOSTAT=ERRCODE) REPLY
      call s_rmcntl (reply)
#
# UPCASE REPLY
      UREPLY=REPLY
      UPC=CASE$A(A$FUPP,UREPLY,INTS(256))
#
# CHECK FOR PRIMOS COMMAND
      IF (REPLY(1:2).EQ.'! ') THEN
        CALL S_COMAND (REPLY(3:))
        GO TO 10
#
# CHECK FOR QUIT
      ELSE IF (UREPLY .EQ. 'QU' .OR. UREPLY .EQ. 'QUIT') THEN
        CALL S_EXIT ('S_QRYC  ',1)
#
# CHECK FOR EXIT
      ELSE IF (UREPLY .EQ. 'EX' .OR. UREPLY .EQ. 'EXIT') THEN
        CALL S_EXIT ('S_QRYC  ',4)
#
# CHECK FOR HELP
      ELSE IF (UREPLY .EQ. '?'   .OR. UREPLY .EQ. 'HE'   .OR.
     C         UREPLY .EQ. 'HEL' .OR. UREPLY .EQ. 'HELP') THEN
#
#     CONVERT IOPT1 TO FIXED LENGTH
        OPT1=IOPT1
        opt1 = ''    !* 04/10/2002 - Decision to shut off help for now
        IF (OPT1 .NE. '') THEN
          CALL S_HELP(OPT1)
          GOTO 10
        ELSE
          CALL S_BADA('Sorry, no help available.',*10)
       END IF
#
# CHECK FOR OOPS
      ELSE IF (UREPLY .EQ. 'OO' .OR.
     C         UREPLY .EQ. 'OOP' .OR. UREPLY .EQ. 'OOPS') THEN
        IF (IOPT2 .EQ. 1) THEN
          RETURN1
        ELSE
          CALL S_BADA('Sorry, OOPS is not available',*10)
        END IF
#
# CHECK FOR NULL RESPONSE
      ELSE IF (REPLY .EQ. '' .AND. IOPT3 .EQ. 1) THEN
        RETURN
#
# DECODE AND CHECK REPLY
      ELSE
        QLNGTH=0
        IF (REPLY.NE.' ') QLNGTH=NLEN$A (REPLY,INTS(256))
        IF (QLNGTH.LT.INTS(INLNMN)) THEN
          WRITE (BADMESS,'(''You must have at least '',I3,
     *    '' characters.'')') INLNMN
          CALL S_BADA (BADMESS,*10)
        ELSEIF (QLNGTH.GT.INTS(INLNMX)) THEN
          WRITE (BADMESS,'(''You cannot have more than '',I3,
     *    '' characters.'')') INLNMX
          CALL S_BADA (BADMESS,*10)
        ELSE
          IF (QLNGTH.EQ.0) THEN
            OTCHRV=' '
          ELSE
            OTCHRV=REPLY(:QLNGTH)
          END IF
          RETURN
        ENDIF
      ENDIF
   20 if (errcode .eq. 4) go to 11 ! Ignore SIGWINCH signal from xterm window resize
      CALL S_BADA('Error reading your reply.',*10)
      RETURN
      END
}
